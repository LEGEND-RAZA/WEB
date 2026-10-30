import express from 'express'
import P from 'pino'
import crypto from 'node:crypto'
import fs from 'node:fs/promises'
import path from 'node:path'
import os from 'node:os'
import { makeWASocket, Browsers, DisconnectReason, useMultiFileAuthState } from '@whiskeysockets/baileys'
import { Boom } from '@hapi/boom'
import { ensureDb, putSession, getSession, updateSession, pruneSessions } from './db.js'
import { mountDeploy, startDeploy, getDeployStatus } from './deploy.js'

process.on('uncaughtException', e => console.error('[PROCESS] Uncaught exception:', e?.stack || e?.message || e))
process.on('unhandledRejection', e => console.error('[PROCESS] Unhandled rejection:', e?.stack || e?.message || e))

const app = express()
const PORT = Number(process.env.PORT) || 3000
const ROOT = process.cwd()
const SESSIONS = path.join(ROOT, 'sessions')
const logger = P({ level: 'silent' })
const jobs = new Map()
const hits = new Map()
const BOOT = new Date().toISOString()

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
const shortNum = n => '...' + String(n).slice(-4)

async function cleanupJob(job) {
  job.cleaned = true
  if (job.sock) {
    try { job.sock.ev.removeAllListeners('connection.update') } catch {}
    try { job.sock.ev.removeAllListeners('creds.update') } catch {}
    try { job.sock.ws?.close() } catch {}
    try { job.sock.end(undefined) } catch {}
  }
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
  if (job.state?.creds?.registered) {
    console.log(`[PAIR SKIP] ${shortNum(job.number)} already registered`)
    return
  }
  job.codeRequesting = true
  try {
    console.log(`[PAIR WAIT] ${shortNum(job.number)} 3s delay before requestPairingCode`)
    await delay(3000)
    if (job.cleaned || job.code) return
    const t0 = Date.now()
    console.log(`[PAIR CALL] ${shortNum(job.number)} requestPairingCode("${job.number}")`)
    const raw = await job.sock.requestPairingCode(job.number)
    const ms = Date.now() - t0
    console.log(`[PAIR RAW] ${shortNum(job.number)} raw=${JSON.stringify(raw)} len=${String(raw).length} type=${typeof raw} took=${ms}ms`)
    if (!raw) throw new Error('WhatsApp returned an empty pairing code')
    job.code = String(raw).replace(/[^A-Z0-9]/gi, '').match(/.{1,4}/g)?.join('-') || String(raw)
    job.rawCode = String(raw)
    job.status = 'waiting'
    job.error = ''
    job.codeIssuedAt = Date.now()
    console.log(`[PAIR OK] ${shortNum(job.number)} display="${job.code}" plain="${String(raw).replace(/[^A-Z0-9]/gi, '')}"`)
  } catch (e) {
    job.code = ''
    job.status = 'error'
    job.error = e?.message || 'Pairing code request failed'
    console.error(`[PAIR FAIL] ${shortNum(job.number)}:`, e?.stack || e?.message || e)
  } finally {
    job.codeRequesting = false
  }
}

async function connectJob(job) {
  if (job.cleaned) return

  const { state, saveCreds } = await useMultiFileAuthState(job.dir)
  job.state = state
  console.log(`[CONNECT] ${shortNum(job.number)} registered=${Boolean(state.creds?.registered)} me=${state.creds?.me?.id || 'none'}`)

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

  // Raw websocket instrumentation
  try {
    const ws = sock.ws
    if (ws) {
      ws.on('open', () => console.log(`[WS OPEN] ${shortNum(job.number)}`))
      ws.on('close', (code, reason) => console.log(`[WS CLOSE] ${shortNum(job.number)} code=${code} reason=${reason?.toString?.() || reason || ''}`))
      ws.on('error', e => console.log(`[WS ERROR] ${shortNum(job.number)}:`, e?.message || e))
      ws.on('ping', () => console.log(`[WS PING] ${shortNum(job.number)}`))
      ws.on('pong', () => console.log(`[WS PONG] ${shortNum(job.number)}`))
      ws.on('unexpected-response', (req, res) => console.log(`[WS UNEXPECTED] ${shortNum(job.number)} status=${res?.statusCode}`))
    } else {
      console.log(`[WS MISSING] ${shortNum(job.number)} sock.ws is undefined`)
    }
  } catch (e) {
    console.log(`[WS ATTACH FAIL] ${shortNum(job.number)}:`, e?.message || e)
  }

  sock.ev.on('creds.update', saveCreds)

  sock.ev.on('connection.update', async update => {
    if (job.cleaned) return
    const { connection, lastDisconnect, qr } = update
    const errCode = new Boom(lastDisconnect?.error)?.output?.statusCode
    console.log(`[CONN] ${shortNum(job.number)} connection=${connection || '-'} qr=${qr ? 'Y' : 'N'} closeCode=${errCode || '-'} closeMsg="${lastDisconnect?.error?.message || ''}" status=${job.status} code="${job.code}"`)

    if (!state.creds.registered && (connection === 'connecting' || qr)) {
      if (!job.code && !job.codeRequesting) await requestPairingCode(job)
    }

    if (connection === 'connecting' && !job.code) job.status = 'connecting'

    if (connection === 'open') {
      console.log(`[OPEN] ${shortNum(job.number)} timeSinceCode=${job.codeIssuedAt ? (Date.now() - job.codeIssuedAt) + 'ms' : 'n/a'}`)
      job.status = 'connected'
      job.error = ''
      try {
        await saveCreds()
        await delay(1500)
        if (job.cleaned) return
        const token = makeToken()
        await putSession({ token, number: job.number, ...(await snapshot(job.dir)) })
        job.token = token
        const jid = `${job.number}@s.whatsapp.net`
        try {
          await sock.sendMessage(jid, { text: token })
          job.sent = true
          console.log(`[SESSION] token sent to ${shortNum(job.number)}`)
        } catch (e) {
          job.sendError = e?.message || 'Could not send session message'
          console.error('[SEND ERROR]', e?.message || e)
        }
        await delay(2500)
        await saveCreds()
        await putSession({ token, number: job.number, ...(await snapshot(job.dir)) })
        job.status = 'ready'
        console.log(`[SESSION] stored in MySQL for ${shortNum(job.number)}`)
        if (job.wantDeploy && process.env.HEROKU_API_KEY) {
          startDeploy(token)
            .then(d => { job.deployId = d.id; console.log(`[DEPLOY] started ${d.id}`) })
            .catch(e => { job.deployError = e?.message || 'deploy fail'; console.error('[DEPLOY] fail:', e?.message || e) })
        }
        setTimeout(() => cleanupJob(job), 5000)
      } catch (e) {
        console.error('[SESSION ERROR]', e?.message || e)
        job.status = 'error'
        job.error = 'Session generation failed. Please try again.'
        await cleanupJob(job)
      }
    }

    if (connection === 'close') {
      console.log(`[CLOSE] ${shortNum(job.number)} code=${errCode || 'unknown'} status=${job.status} codeAge=${job.codeIssuedAt ? (Date.now() - job.codeIssuedAt) + 'ms' : 'n/a'}`)
      if (errCode === DisconnectReason.loggedOut) {
        job.status = 'error'
        job.error = 'WhatsApp logged out'
        return cleanupJob(job)
      }
      if (['ready', 'connected', 'error'].includes(job.status) || job.cleaned || job.reconnecting) return
      job.reconnecting = true
      setTimeout(async () => {
        job.reconnecting = false
        if (job.cleaned) return
        console.log(`[RECONNECT] ${shortNum(job.number)}`)
        try { await connectJob(job) }
        catch (e) {
          job.status = 'error'
          job.error = e?.message || 'Reconnect failed'
          await cleanupJob(job)
        }
      }, 2000)
    }
  })
}

app.get('/health', (req, res) => res.json({ success: true, status: 'online' }))

app.get('/api/diag', async (req, res) => {
  let baileysVersion = 'unknown'
  try {
    const pkg = await import('@whiskeysockets/baileys/package.json', { with: { type: 'json' } }).catch(() => null)
    baileysVersion = pkg?.default?.version || 'import failed'
  } catch (e) { baileysVersion = 'error: ' + (e?.message || e) }

  let wsVersion = 'unknown'
  try {
    const pkg = await import('ws/package.json', { with: { type: 'json' } }).catch(() => null)
    wsVersion = pkg?.default?.version || 'import failed'
  } catch (e) { wsVersion = 'error: ' + (e?.message || e) }

  res.json({
    boot: BOOT,
    node: process.version,
    platform: process.platform,
    arch: process.arch,
    uptime_sec: Math.round(process.uptime()),
    baileysVersion,
    wsVersion,
    dyno: process.env.DYNO,
    herokuAppName: process.env.HEROKU_APP_NAME,
    AUTO_DEPLOY: process.env.AUTO_DEPLOY,
    HEROKU_API_KEY_set: Boolean(process.env.HEROKU_API_KEY),
    activeJobs: [...jobs.values()].map(j => ({
      id: j.id, number: shortNum(j.number), status: j.status, code: j.code,
      rawCode: j.rawCode, codeAgeMs: j.codeIssuedAt ? Date.now() - j.codeIssuedAt : null,
      sent: j.sent, error: j.error, sendError: j.sendError, cleaned: j.cleaned
    }))
  })
})

app.get('/', (req, res) => res.sendFile(path.join(ROOT, 'public', 'index.html')))

app.post('/api/pair', async (req, res) => {
  try {
    const number = cleanNumber(req.body?.number)
    console.log(`[PAIR API] number=${number} (${shortNum(number)}) len=${number.length} deploy=${req.body?.deploy} ua="${req.headers['user-agent'] || ''}"`)
    if (number.length < 7 || number.length > 15) {
      return res.status(400).json({ success: false, error: 'Invalid WhatsApp number' })
    }
    try { await ensureDb() } catch (e) {
      console.error('[DB]', e?.message || e)
      return res.status(503).json({ success: false, error: 'Session storage is unavailable. Try again shortly.' })
    }
    const now = Date.now()
    const list = (hits.get(req.ip) || []).filter(t => now - t < 10 * 60 * 1000)
    list.push(now)
    hits.set(req.ip, list)
    if (list.length > 8) return res.status(429).json({ success: false, error: 'Too many attempts. Try again in a few minutes.' })
    const veryRecent = [...jobs.values()].find(j => j.number === number && Date.now() - j.createdAt < 2000)
    if (veryRecent) return res.json({ success: true, id: veryRecent.id, code: veryRecent.code, status: veryRecent.status })
    for (const [id, j] of [...jobs.entries()]) if (j.number === number) { cleanupJob(j).catch(() => {}); jobs.delete(id) }
    const running = [...jobs.values()].filter(j => !['ready', 'error'].includes(j.status))
    if (running.length >= 20) return res.status(429).json({ success: false, error: 'The server is busy. Try again in a minute.' })
    const job = {
      id: makeId(), number,
      dir: path.join(SESSIONS, crypto.randomBytes(8).toString('hex')),
      code: '', rawCode: '', codeIssuedAt: 0,
      token: '', status: 'starting', sent: false, error: '', sendError: '',
      codeRequesting: false, reconnecting: false,
      wantDeploy: process.env.AUTO_DEPLOY !== 'false' && req.body?.deploy !== false,
      deployId: '', deployError: '', cleaned: false, sock: null, state: null,
      createdAt: Date.now()
    }
    await fs.mkdir(job.dir, { recursive: true })
    jobs.set(job.id, job)
    console.log(`[PAIR JOB] created ${job.id}`)
    connectJob(job).catch(async e => {
      job.status = 'error'
      job.error = e?.message || 'Could not start pairing'
      await cleanupJob(job)
    })
    for (let i = 0; i < 48; i++) {
      if (job.code || job.status === 'error') break
      await delay(250)
    }
    console.log(`[PAIR RESP] job=${job.id} code="${job.code}" status=${job.status}`)
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
    success: true, id: job.id, status: job.status, code: job.code,
    session: job.status === 'ready' ? job.token : '',
    sent: job.sent, error: job.error, sendError: job.sendError,
    deploy: job.deployId ? { id: job.deployId, ...getDeployStatus(job.deployId) } : null,
    deployError: job.deployError
  })
})

app.get('/api/session/:token', async (req, res) => {
  const token = String(req.params.token || '').trim()
  if (!/^RAZA_[A-Za-z0-9_-]{10,100}$/.test(token)) return res.status(400).json({ success: false, error: 'Invalid session token' })
  try {
    const session = await getSession(token)
    if (!session) return res.status(404).json({ success: false, error: 'Session not found' })
    res.set('Cache-Control', 'no-store')
    res.json({ success: true, version: session.version, files: session.files })
  } catch (e) { console.error('[SESSION API]', e?.message || e); res.status(500).json({ success: false, error: 'Could not retrieve session' }) }
})

app.put('/api/session/:token', async (req, res) => {
  const token = String(req.params.token || '').trim()
  if (!/^RAZA_[A-Za-z0-9_-]{10,100}$/.test(token)) return res.status(400).json({ success: false, error: 'Invalid session token' })
  const files = req.body?.files
  const names = files && typeof files === 'object' && !Array.isArray(files) ? Object.keys(files) : []
  if (!names.length) return res.status(400).json({ success: false, error: 'No session files received' })
  if (names.length > 20000) return res.status(400).json({ success: false, error: 'Too many session files' })
  if (!names.includes('creds.json')) return res.status(400).json({ success: false, error: 'creds.json missing from payload' })
  const NAME_RE = /^[A-Za-z0-9._@-]{1,200}$/
  const badName = names.find(n => typeof n !== 'string' || !NAME_RE.test(n) || n.includes('..') || n.includes('/') || n.includes('\\') || n.startsWith('.') || typeof files[n] !== 'string' || files[n].length === 0)
  if (badName !== undefined) {
    console.error('[SESSION PUT] rejected — invalid filename:', JSON.stringify(badName))
    return res.status(400).json({ success: false, error: 'Invalid filename in session payload' })
  }
  let paired = false
  try {
    const decoded = Buffer.from(files['creds.json'], 'base64').toString('utf8')
    paired = Boolean(JSON.parse(decoded)?.me?.id)
  } catch (e) { console.error('[SESSION PUT] creds.json decode failed:', e?.message || e) }
  if (!paired) return res.status(400).json({ success: false, error: 'creds.json is not a valid paired session' })
  try {
    if (!(await updateSession(token, files))) return res.status(404).json({ success: false, error: 'Session not found' })
    console.log(`[SESSION PUT] synced ${names.length} files for ${token.slice(0, 12)}…`)
    res.json({ success: true })
  } catch (e) { console.error('[SESSION PUT]', e?.message || e); res.status(500).json({ success: false, error: 'Could not save session' }) }
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
  console.log(`[BOOT] node=${process.version} dyno=${process.env.DYNO || '-'}`)
  ensureDb().then(() => console.log('[+] MySQL session storage ready.')).catch(e => console.error('[DB]', e?.message || e))
})
