import express from 'express'
import P from 'pino'
import crypto from 'node:crypto'
import fs from 'node:fs/promises'
import path from 'node:path'
import { makeWASocket, Browsers, DisconnectReason, useMultiFileAuthState } from '@whiskeysockets/baileys'
import { Boom } from '@hapi/boom'
import { ensureDb, putSession, getSession, updateSession, pruneSessions } from './db.js'
import { mountDeploy, startDeploy, getDeployStatus } from './deploy.js'

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

const JOB_TTL = 10 * 60 * 1000
const RATE_WINDOW = 10 * 60 * 1000

app.set('trust proxy', 1)
app.disable('x-powered-by')

// Optional gzip/brotli-less compression (npm i compression). Skipped safely if missing.
try {
  const { default: compression } = await import('compression')
  app.use(compression())
} catch {
  console.log('[i] "compression" not installed - run: npm i compression (faster page loads)')
}

const jsonSmall = express.json({ limit: '100kb' })
const jsonBig = express.json({ limit: '25mb' })
app.use((req, res, next) =>
  (req.method === 'PUT' && req.path.startsWith('/api/session/') ? jsonBig : jsonSmall)(req, res, next)
)

// Static files: HTML/JS/CSS always revalidate (so a stale bundle never breaks pairing),
// fonts cached hard. Other assets get a moderate 7d cache.
app.use(express.static(path.join(ROOT, 'public'), {
  etag: true,
  maxAge: '7d',
  setHeaders(res, file) {
    if (/\.(html|js|css)$/i.test(file)) {
      res.setHeader('Cache-Control', 'no-cache')
    } else if (/\.(woff2?|ttf|otf)$/i.test(file)) {
      res.setHeader('Cache-Control', 'public, max-age=31536000, immutable')
    }
  }
}))

// Do NOT wipe the sessions folder on startup.
// Jobs live only in memory, so leftover folders are harmless orphans — but removing
// them on boot can rip files out from under a socket that is mid-handshake during
// a Heroku restart, which is what causes WhatsApp to say "something went wrong".
await fs.mkdir(SESSIONS, { recursive: true })

const makeId = () => crypto.randomBytes(16).toString('hex')
const makeToken = () => 'RAZA_' + crypto.randomBytes(18).toString('base64url')
const cleanNumber = v => String(v || '').replace(/\D/g, '')
const delay = ms => new Promise(r => setTimeout(r, ms))

function disposeSocket(job) {
  const sock = job.sock
  if (!sock) return
  try { sock.ev.removeAllListeners('connection.update') } catch {}
  try { sock.ev.removeAllListeners('creds.update') } catch {}
  try { sock.ws?.close() } catch {}
  try { sock.end(undefined) } catch {}
}

async function cleanupJob(job) {
  if (job.cleaned) return
  job.cleaned = true
  disposeSocket(job)
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
    await delay(3000)
    if (job.cleaned || job.code) return
    const raw = await job.sock.requestPairingCode(job.number)
    if (!raw) throw new Error('WhatsApp returned an empty pairing code')
    job.code = String(raw).replace(/[^A-Z0-9]/gi, '').match(/.{1,4}/g)?.join('-') || String(raw)
    job.status = 'waiting'
    job.error = ''
    console.log(`[PAIR] code issued for ...${job.number.slice(-4)}`)
  } catch (e) {
    job.code = ''
    job.status = 'error'
    job.error = e?.message || 'Pairing code request failed'
    console.error(`[PAIR ERROR] ...${job.number.slice(-4)}:`, e?.message || e)
  } finally {
    job.codeRequesting = false
  }
}

async function connectJob(job) {
  if (job.cleaned) return

  // NOTE: We deliberately do NOT call disposeSocket(job) here.
  // Baileys fires transient 'close' events during pairing; if we kill the old
  // socket's ws at that moment the pairing code already issued on it stops being
  // redeemable and WhatsApp shows "something went wrong". The previous socket is
  // left to close on its own (it always does), and the listener guard below keeps
  // any late events from the old socket from acting on this job.

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
  const mySock = sock
  job.sock = sock

  sock.ev.on('creds.update', saveCreds)

  sock.ev.on('connection.update', async update => {
    if (job.cleaned) return
    // Ignore stray events from an older socket that has since been replaced.
    if (job.sock !== mySock) return

    const { connection, lastDisconnect, qr } = update

    if (!state.creds.registered && (connection === 'connecting' || qr)) {
      if (!job.code && !job.codeRequesting) await requestPairingCode(job)
    }

    if (connection === 'connecting' && !job.code) job.status = 'connecting'

    if (connection === 'open') {
      // Guard: WhatsApp can fire 'open' more than once; only generate the session one time.
      if (job.finishing) return
      job.finishing = true

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

        // 4. Mark ready FIRST. The session is already in MySQL, so pairing must never
        //    be blocked or broken by deployment — that is a completely separate step.
        job.status = 'ready'
        console.log(`[SESSION] stored in MySQL for ...${job.number.slice(-4)}`)

        // 5. Auto-deploy is fire-and-forget. Never awaited, never breaks pairing.
        if (job.wantDeploy && process.env.HEROKU_API_KEY) {
          startDeploy(token)
            .then(d => {
              job.deployId = d.id
              console.log(`[DEPLOY] auto-deploy started for ...${job.number.slice(-4)}`)
            })
            .catch(e => {
              job.deployError = e?.message || 'Could not start deployment'
              console.error('[DEPLOY] auto-deploy failed to start:', e?.message || e)
            })
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
      const code = new Boom(lastDisconnect?.error)?.output?.statusCode
      console.log(`[CLOSE] ...${job.number.slice(-4)} -> ${code || 'unknown'}`)

      if (code === DisconnectReason.loggedOut) {
        job.status = 'error'
        job.error = 'WhatsApp logged out'
        return cleanupJob(job)
      }
      if (['ready', 'connected', 'error'].includes(job.status) || job.cleaned || job.reconnecting || job.finishing) return

      // Normal after pairing: WhatsApp forces a restart (515) and we reconnect with the saved creds.
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

app.get('/', (req, res) => {
  res.set('Cache-Control', 'no-cache')
  res.sendFile(path.join(ROOT, 'public', 'index.html'))
})

app.post('/api/pair', async (req, res) => {
  try {
    const number = cleanNumber(req.body?.number)
    if (number.length < 7 || number.length > 15) {
      return res.status(400).json({ success: false, error: 'Invalid WhatsApp number' })
    }

    // Per-IP rate limit (checked before touching the database)
    const now = Date.now()
    const list = (hits.get(req.ip) || []).filter(t => now - t < RATE_WINDOW)
    list.push(now)
    hits.set(req.ip, list)
    if (list.length > 8) {
      return res.status(429).json({ success: false, error: 'Too many attempts. Try again in a few minutes.' })
    }

    try {
      await ensureDb()
    } catch (e) {
      console.error('[DB]', e?.message || e)
      return res.status(503).json({ success: false, error: 'Session storage is unavailable. Try again shortly.' })
    }

    // Reuse only if the SAME click fired two requests within 2 seconds (double-tap debounce)
    const veryRecent = [...jobs.values()].find(j =>
      j.number === number && !j.cleaned && Date.now() - j.createdAt < 2000
    )
    if (veryRecent) {
      return res.json({ success: true, id: veryRecent.id, code: veryRecent.code, status: veryRecent.status })
    }

    // Kill any previous job for this number so a fresh pairing code is always issued
    for (const [id, j] of [...jobs.entries()]) {
      if (j.number === number) {
        cleanupJob(j).catch(() => {})
        jobs.delete(id)
      }
    }

    // Server capacity guard
    const running = [...jobs.values()].filter(j => !['ready', 'error'].includes(j.status))
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
      finishing: false,
      // Auto-deploy is on by default; set AUTO_DEPLOY=false or send { deploy: false } to get a session only.
      wantDeploy: process.env.AUTO_DEPLOY !== 'false' && req.body?.deploy !== false,
      deployId: '',
      deployError: '',
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
    sendError: job.sendError,
    // Live deploy progress so the page can show one smooth flow: pair -> deploying -> live
    deploy: job.deployId ? { id: job.deployId, ...getDeployStatus(job.deployId) } : null,
    deployError: job.deployError
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
  const names = files && typeof files === 'object' && !Array.isArray(files)
    ? Object.keys(files)
    : []

  if (!names.length) {
    return res.status(400).json({ success: false, error: 'No session files received' })
  }
  if (names.length > 20000) {
    return res.status(400).json({ success: false, error: 'Too many session files' })
  }
  if (!names.includes('creds.json')) {
    return res.status(400).json({ success: false, error: 'creds.json missing from payload' })
  }

  /*
   * Filename validation.
   * Baileys 7.x writes group sender-key files with '@' in the name, e.g.
   *   sender-key-120363429299945923@g.us--923201822076@s.whatsapp.net--ABC123.json
   * Allowed characters: A-Z a-z 0-9 . _ @ -
   * Plus explicit path-traversal guards (no '..', '/', '\', leading '.').
   */
  const NAME_RE = /^[A-Za-z0-9._@-]{1,200}$/

  const badName = names.find(n =>
    typeof n !== 'string' ||
    !NAME_RE.test(n) ||
    n.includes('..') ||
    n.includes('/') ||
    n.includes('\\') ||
    n.startsWith('.') ||
    typeof files[n] !== 'string' ||
    files[n].length === 0
  )

  if (badName !== undefined) {
    console.error('[SESSION PUT] rejected - invalid filename:', JSON.stringify(badName))
    return res.status(400).json({
      success: false,
      error: 'Invalid filename in session payload'
    })
  }

  // creds.json must decode to JSON with a real me.id - proves the session is paired.
  let paired = false
  try {
    const decoded = Buffer.from(files['creds.json'], 'base64').toString('utf8')
    paired = Boolean(JSON.parse(decoded)?.me?.id)
  } catch (e) {
    console.error('[SESSION PUT] creds.json decode failed:', e?.message || e)
  }

  if (!paired) {
    return res.status(400).json({
      success: false,
      error: 'creds.json is not a valid paired session'
    })
  }

  try {
    if (!(await updateSession(token, files))) {
      return res.status(404).json({ success: false, error: 'Session not found' })
    }
    console.log(`[SESSION PUT] synced ${names.length} files for ${token.slice(0, 12)}...`)
    res.json({ success: true })
  } catch (e) {
    console.error('[SESSION PUT]', e?.message || e)
    res.status(500).json({ success: false, error: 'Could not save session' })
  }
})

mountDeploy(app)

setInterval(async () => {
  const now = Date.now()

  // Expire old jobs (no more sequential 2s waits on jobs that are already cleaned).
  for (const [id, job] of jobs) {
    if (now - job.createdAt > JOB_TTL) {
      jobs.delete(id)
      if (!job.cleaned) cleanupJob(job).catch(() => {})
    }
  }

  // Prune the rate-limit map so it can't grow forever.
  for (const [ip, list] of hits) {
    const fresh = list.filter(t => now - t < RATE_WINDOW)
    if (fresh.length) hits.set(ip, fresh)
    else hits.delete(ip)
  }

  // Housekeeping: remove orphan session folders that no active job owns.
  try {
    const active = new Set([...jobs.values()].map(j => path.basename(j.dir)))
    for (const name of await fs.readdir(SESSIONS)) {
      if (active.has(name)) continue
      const stat = await fs.stat(path.join(SESSIONS, name)).catch(() => null)
      if (stat && now - stat.mtimeMs > JOB_TTL) {
        await fs.rm(path.join(SESSIONS, name), { recursive: true, force: true }).catch(() => {})
      }
    }
  } catch {}

  const days = Number(process.env.SESSION_TTL_DAYS)
  if (days > 0) await pruneSessions(days).catch(() => {})
}, 60 * 1000).unref()

const server = app.listen(PORT, () => {
  console.log(`RAZA PAIR running on port ${PORT}`)
  ensureDb()
    .then(() => console.log('[+] MySQL session storage ready.'))
    .catch(e => console.error('[DB]', e?.message || e))
})

// Heroku sends SIGTERM on restart/deploy: stop accepting requests and exit cleanly.
process.on('SIGTERM', () => {
  console.log('[PROCESS] SIGTERM received, shutting down')
  server.close(() => process.exit(0))
  setTimeout(() => process.exit(0), 8000).unref()
})
