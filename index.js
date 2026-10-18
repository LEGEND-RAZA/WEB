import express from 'express'
import P from 'pino'
import crypto from 'node:crypto'
import fs from 'node:fs/promises'
import path from 'node:path'
import { makeWASocket, Browsers, DisconnectReason, useMultiFileAuthState } from '@whiskeysockets/baileys'
import { Boom } from '@hapi/boom'
import { ensureDb, putSession, getSession, updateSession, pruneSessions } from './db.js'
import { mountDeploy } from './deploy.js'

// Never let a stray WhatsApp-library error take the whole site down.
process.on('uncaughtException', e => console.error('[PROCESS] Uncaught exception:', e?.stack || e?.message || e))
process.on('unhandledRejection', e => console.error('[PROCESS] Unhandled rejection:', e?.stack || e?.message || e))

const app = express()
const PORT = Number(process.env.PORT) || 3000
const ROOT = process.cwd()
const SESSIONS = path.join(ROOT, 'sessions')
const logger = P({ level: 'silent' })
const jobs = new Map()
const hits = new Map()

app.set('trust proxy', 1)
const jsonSmall = express.json({ limit: '100kb' })
const jsonBig = express.json({ limit: '25mb' })
app.use((req, res, next) =>
  (req.method === 'PUT' && req.path.startsWith('/api/session/') ? jsonBig : jsonSmall)(req, res, next)
)
app.use(express.static(path.join(ROOT, 'public')))

await fs.mkdir(SESSIONS, { recursive: true })

const makeId = () => crypto.randomBytes(16).toString('hex')
const makeToken = () => 'RAZA_' + crypto.randomBytes(18).toString('base64url')
const cleanNumber = v => String(v || '').replace(/\D/g, '')
const delay = ms => new Promise(r => setTimeout(r, ms))

async function cleanupJob(job) {
  job.cleaned = true
  if (job.sock) {
    try { job.sock.ev.removeAllListeners('connection.update') } catch {}
    try { job.sock.ev.removeAllListeners('creds.update') } catch {}
    try { job.sock.ws?.close() } catch {}
    try { job.sock.end(undefined) } catch {}
  }
  // Give in-flight key writes a moment to finish before the folder disappears.
  await delay(2000)
  await fs.rm(job.dir, { recursive: true, force: true }).catch(() => {})
}

async function snapshot(dir) {
  const files = {}
  for (const name of await fs.readdir(dir)) {
    const file = path.join(dir, name)
    if ((await fs.stat(file)).isFile()) files[name] = (await fs.readFile(file)).toString('base64')
  }
  if (!files['creds.json']) throw new Error('Session files were not created')
  return { version: 1, files }
}

async function requestPairingCode(job) {
  if (job.cleaned || job.code || job.status === 'error' || job.codeRequesting) return
  if (job.state?.creds?.registered) return
  job.codeRequesting = true
  try {
    await delay(2000)
    if (job.cleaned || job.code) return
    const raw = await job.sock.requestPairingCode(job.number)
    if (!raw) throw new Error('WhatsApp returned an empty pairing code')
    job.code = String(raw).replace(/[^A-Z0-9]/gi, '').match(/.{1,4}/g)?.join('-') || String(raw)
    job.status = 'waiting'
    job.error = ''
    console.log(`[PAIR] code issued for ...${job.number.slice(-4)}`)
  } catch (e) {
    job.code = ''
    job.status = 'connecting'
    job.error = e?.message || 'Pairing code request failed'
    console.error(`[PAIR ERROR] ...${job.number.slice(-4)}:`, e?.message || e)
  } finally {
    job.codeRequesting = false
  }
}

async function connectJob(job) {
  if (job.cleaned) return

  const { state, saveCreds } = await useMultiFileAuthState(job.dir)
  job.state = state

  const sock = makeWASocket({
    auth: state,
    browser: Browsers.macOS('Chrome'),
    printQRInTerminal: false,
    logger,
    markOnlineOnConnect: false,
    syncFullHistory: false,
    generateHighQualityLinkPreview: false,
    connectTimeoutMs: 60000,
    defaultQueryTimeoutMs: 60000,
    keepAliveIntervalMs: 30000
  })
  job.sock = sock

  sock.ev.on('creds.update', saveCreds)

  sock.ev.on('connection.update', async update => {
    if (job.cleaned) return
    const { connection, lastDisconnect, qr } = update

    if (!state.creds.registered && (connection === 'connecting' || qr)) {
      if (!job.code && !job.codeRequesting) await requestPairingCode(job)
    }

    if (connection === 'connecting' && !job.code) job.status = 'connecting'

    if (connection === 'open') {
      job.status = 'connected'
      job.error = ''
      try {
        await saveCreds()
        await delay(1500)
        if (job.cleaned) return

        // 1. Store the session first so the token is valid straight away.
        const token = makeToken()
        await putSession({ token, number: job.number, ...(await snapshot(job.dir)) })
        job.token = token

        // 2. Send it to the user's own chat (single message).
        const jid = `${job.number}@s.whatsapp.net`
        try {
          await sock.sendMessage(jid, { text: token })
          job.sent = true
          console.log(`[SESSION] sent to ...${job.number.slice(-4)}`)
        } catch (e) {
          job.sendError = e?.message || 'Could not send session message'
          console.error('[SEND ERROR]', e?.message || e)
        }

        // 3. Save the final state (includes keys created while sending), then finish.
        await delay(2500)
        await saveCreds()
        await putSession({ token, number: job.number, ...(await snapshot(job.dir)) })
        job.status = 'ready'
        console.log(`[SESSION] stored in MySQL for ...${job.number.slice(-4)}`)
        setTimeout(() => cleanupJob(job), 5000)
      } catch (e) {
        console.error('[SESSION ERROR]', e?.message || e)
        job.status = 'error'
        job.error = 'Session generation failed. Please try again.'
        await cleanupJob(job)
      }
    }

    if (connection === 'close') {
      const code = new Boom(lastDisconnect?.error)?.output?.statusCode
      console.log(`[CLOSE] ...${job.number.slice(-4)} -> ${code || 'unknown'}`)

      if (code === DisconnectReason.loggedOut) {
        job.status = 'error'
        job.error = 'WhatsApp logged out'
        return cleanupJob(job)
      }
      if (['ready', 'connected', 'error'].includes(job.status) || job.cleaned || job.reconnecting) return

      job.reconnecting = true
      setTimeout(async () => {
        job.reconnecting = false
        if (job.cleaned) return
        try {
          await connectJob(job)
        } catch (e) {
          job.status = 'error'
          job.error = e?.message || 'Reconnect failed'
          await cleanupJob(job)
        }
      }, 2000)
    }
  })
}

app.get('/health', (req, res) => res.json({ success: true, status: 'online' }))

app.get('/', (req, res) => res.sendFile(path.join(ROOT, 'public', 'index.html')))

app.post('/api/pair', async (req, res) => {
  try {
    const number = cleanNumber(req.body?.number)
    if (number.length < 7 || number.length > 15) {
      return res.status(400).json({ success: false, error: 'Invalid WhatsApp number' })
    }

    try {
      await ensureDb()
    } catch (e) {
      console.error('[DB]', e?.message || e)
      return res.status(503).json({ success: false, error: 'Session storage is unavailable. Try again shortly.' })
    }

    const now = Date.now()
    const list = (hits.get(req.ip) || []).filter(t => now - t < 10 * 60 * 1000)
    list.push(now)
    hits.set(req.ip, list)
    if (list.length > 8) {
      return res.status(429).json({ success: false, error: 'Too many attempts. Try again in a few minutes.' })
    }

    const running = [...jobs.values()].filter(j => !['ready', 'error'].includes(j.status))
    const existing = running.find(j => j.number === number)
    if (existing) {
      return res.json({ success: true, id: existing.id, code: existing.code, status: existing.status })
    }
    if (running.length >= 20) {
      return res.status(429).json({ success: false, error: 'The server is busy. Try again in a minute.' })
    }

    const job = {
      id: makeId(),
      number,
      dir: path.join(SESSIONS, crypto.randomBytes(8).toString('hex')),
      code: '',
      token: '',
      status: 'starting',
      sent: false,
      error: '',
      sendError: '',
      codeRequesting: false,
      reconnecting: false,
      cleaned: false,
      sock: null,
      state: null,
      createdAt: Date.now()
    }

    await fs.mkdir(job.dir, { recursive: true })
    jobs.set(job.id, job)

    connectJob(job).catch(async e => {
      job.status = 'error'
      job.error = e?.message || 'Could not start pairing'
      await cleanupJob(job)
    })

    for (let i = 0; i < 48; i++) {
      if (job.code || job.status === 'error') break
      await delay(250)
    }

    res.json({ success: true, id: job.id, code: job.code, status: job.status })
  } catch (e) {
    res.status(500).json({ success: false, error: e?.message || 'Internal server error' })
  }
})

app.get('/api/status/:id', (req, res) => {
  const job = jobs.get(req.params.id)
  if (!job) return res.status(404).json({ success: false, error: 'Pairing session not found' })
  res.set('Cache-Control', 'no-store')
  res.json({
    success: true,
    id: job.id,
    status: job.status,
    code: job.code,
    session: job.status === 'ready' ? job.token : '',
    sent: job.sent,
    error: job.error,
    sendError: job.sendError
  })
})

app.get('/api/session/:token', async (req, res) => {
  const token = String(req.params.token || '').trim()
  if (!/^RAZA_[A-Za-z0-9_-]{10,100}$/.test(token)) {
    return res.status(400).json({ success: false, error: 'Invalid session token' })
  }
  try {
    const session = await getSession(token)
    if (!session) return res.status(404).json({ success: false, error: 'Session not found' })
    res.set('Cache-Control', 'no-store')
    res.json({ success: true, version: session.version, files: session.files })
  } catch (e) {
    console.error('[SESSION API]', e?.message || e)
    res.status(500).json({ success: false, error: 'Could not retrieve session' })
  }
})

// The bot uploads its latest keys here so a Heroku restart resumes from fresh state.
app.put('/api/session/:token', async (req, res) => {
  const token = String(req.params.token || '').trim()
  if (!/^RAZA_[A-Za-z0-9_-]{10,100}$/.test(token)) {
    return res.status(400).json({ success: false, error: 'Invalid session token' })
  }

  const files = req.body?.files
  const names = files && typeof files === 'object' ? Object.keys(files) : []
  const valid =
    names.length > 0 &&
    names.length <= 20000 &&
    names.includes('creds.json') &&
    names.every(n => /^[A-Za-z0-9._-]{1,200}$/.test(n) && typeof files[n] === 'string')

  let paired = false
  try {
    paired = Boolean(JSON.parse(Buffer.from(files['creds.json'], 'base64').toString('utf8'))?.me?.id)
  } catch {}

  if (!valid || !paired) return res.status(400).json({ success: false, error: 'Invalid session files' })

  try {
    if (!(await updateSession(token, files))) {
      return res.status(404).json({ success: false, error: 'Session not found' })
    }
    res.json({ success: true })
  } catch (e) {
    console.error('[SESSION PUT]', e?.message || e)
    res.status(500).json({ success: false, error: 'Could not save session' })
  }
})

mountDeploy(app)

setInterval(async () => {
  const jobExpiry = Date.now() - 10 * 60 * 1000
  for (const [id, job] of jobs) {
    if (job.createdAt < jobExpiry) {
      await cleanupJob(job)
      jobs.delete(id)
    }
  }
  const days = Number(process.env.SESSION_TTL_DAYS)
  if (days > 0) await pruneSessions(days).catch(() => {})
}, 60 * 1000).unref()

app.listen(PORT, () => {
  console.log(`RAZA PAIR running on port ${PORT}`)
  ensureDb()
    .then(() => console.log('[+] MySQL session storage ready.'))
    .catch(e => console.error('[DB]', e?.message || e))
})
