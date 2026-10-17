import express from 'express'
import P from 'pino'
import crypto from 'node:crypto'
import fs from 'node:fs/promises'
import path from 'node:path'
import mysql from 'mysql2/promise'

import {
  makeWASocket,
  Browsers,
  DisconnectReason,
  useMultiFileAuthState
} from '@whiskeysockets/baileys'
import { Boom } from '@hapi/boom'
import { mountDeploy } from './deploy.js'

const app = express()
const PORT = Number(process.env.PORT) || 3000
const ROOT = process.cwd()
const SESSIONS = path.join(ROOT, 'sessions')
const SESSION_TTL = 30 * 24 * 60 * 60 * 1000
const JOB_TTL = 10 * 60 * 1000
const logger = P({ level: 'silent' })

const jobs = new Map()
const hits = new Map()

const requiredDbVars = ['DB_HOST', 'DB_USER', 'DB_PASSWORD']
for (const key of requiredDbVars) {
  if (!process.env[key]) throw new Error(`Missing required environment variable: ${key}`)
}

// SESSION_ENCRYPTION_KEY must be exactly 64 hexadecimal characters (32 bytes).
const encryptionKeyHex = process.env.SESSION_ENCRYPTION_KEY || ''
if (!/^[a-fA-F0-9]{64}$/.test(encryptionKeyHex)) {
  throw new Error('SESSION_ENCRYPTION_KEY must be a 64-character hexadecimal key (32 bytes).')
}
const encryptionKey = Buffer.from(encryptionKeyHex, 'hex')

const pool = mysql.createPool({
  host: process.env.DB_HOST,
  port: Number(process.env.DB_PORT || 3306),
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME || 'defaultdb',
  ssl: process.env.DB_SSL === 'false' ? undefined : { rejectUnauthorized: false },
  waitForConnections: true,
  connectionLimit: 5,
  queueLimit: 0,
  connectTimeout: 15000,
  timezone: 'Z'
})

app.set('trust proxy', 1)
app.use(express.json({ limit: '25mb' }))
app.use(express.static(path.join(ROOT, 'public')))
app.use((req, res, next) => {
  res.set('X-Content-Type-Options', 'nosniff')
  next()
})

await fs.mkdir(SESSIONS, { recursive: true })
await initializeDatabase()

async function initializeDatabase() {
  await pool.execute(`
    CREATE TABLE IF NOT EXISTS raza_pair_sessions (
      token_hash CHAR(64) NOT NULL PRIMARY KEY,
      phone_number VARCHAR(20) NOT NULL,
      version INT NOT NULL DEFAULT 1,
      encrypted_payload MEDIUMTEXT NOT NULL,
      created_at DATETIME(3) NOT NULL,
      expires_at DATETIME(3) NOT NULL,
      INDEX idx_raza_pair_sessions_expiry (expires_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  `)
  await pool.execute('DELETE FROM raza_pair_sessions WHERE expires_at <= UTC_TIMESTAMP(3)')
  console.log('[SQL] Session table is ready.')
}

function makeId() {
  return crypto.randomBytes(16).toString('hex')
}

function makeToken() {
  return 'RAZA_' + crypto.randomBytes(18).toString('base64url')
}

function tokenHash(token) {
  return crypto.createHash('sha256').update(token).digest('hex')
}

function encryptPayload(value) {
  const iv = crypto.randomBytes(12)
  const cipher = crypto.createCipheriv('aes-256-gcm', encryptionKey, iv)
  const encrypted = Buffer.concat([
    cipher.update(JSON.stringify(value), 'utf8'),
    cipher.final()
  ])
  return JSON.stringify({
    v: 1,
    iv: iv.toString('base64'),
    tag: cipher.getAuthTag().toString('base64'),
    data: encrypted.toString('base64')
  })
}

function decryptPayload(serialized) {
  const record = JSON.parse(serialized)
  if (record?.v !== 1 || !record.iv || !record.tag || !record.data) {
    throw new Error('Stored session payload has an unsupported format.')
  }
  const decipher = crypto.createDecipheriv(
    'aes-256-gcm',
    encryptionKey,
    Buffer.from(record.iv, 'base64')
  )
  decipher.setAuthTag(Buffer.from(record.tag, 'base64'))
  const plaintext = Buffer.concat([
    decipher.update(Buffer.from(record.data, 'base64')),
    decipher.final()
  ]).toString('utf8')
  return JSON.parse(plaintext)
}

function cleanNumber(value) {
  return String(value || '').replace(/\D/g, '')
}

function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms))
}

function isTerminal(job) {
  return ['ready', 'error'].includes(job.status)
}

function limited(ip) {
  const now = Date.now()
  const list = (hits.get(ip) || []).filter(t => now - t < 10 * 60 * 1000)
  list.push(now)
  hits.set(ip, list)
  return list.length > 5
}

async function cleanupJob(jobId) {
  const job = jobs.get(jobId)
  if (!job || job.cleaned) return
  job.cleaned = true

  if (job.sock) {
    try { job.sock.ev.removeAllListeners('connection.update') } catch {}
    try { job.sock.ev.removeAllListeners('creds.update') } catch {}
    try { job.sock.ws?.close() } catch {}
    try { job.sock.end(undefined) } catch {}
  }

  try {
    await fs.rm(job.dir, { recursive: true, force: true })
  } catch (error) {
    console.error('[PAIR] Temporary directory cleanup failed:', error?.message || error)
  }
}

async function encodeSession(authDir) {
  const files = {}
  const names = await fs.readdir(authDir)
  for (const name of names) {
    if (!/^[A-Za-z0-9._-]+$/.test(name)) continue
    const file = path.join(authDir, name)
    const stat = await fs.stat(file)
    if (stat.isFile()) files[name] = (await fs.readFile(file)).toString('base64')
  }
  if (!files['creds.json']) throw new Error('Baileys creds.json was not found; refusing to save an incomplete session.')
  return { version: 1, files }
}

async function saveSession(job) {
  const data = await encodeSession(job.dir)
  const token = makeToken()
  const now = Date.now()
  const createdAt = new Date(now)
  const expiresAt = new Date(now + SESSION_TTL)
  const payload = encryptPayload({ version: data.version, files: data.files })

  await pool.execute(
    `INSERT INTO raza_pair_sessions
      (token_hash, phone_number, version, encrypted_payload, created_at, expires_at)
     VALUES (?, ?, ?, ?, ?, ?)`,
    [tokenHash(token), job.number, data.version, payload, createdAt, expiresAt]
  )

  job.token = token
  console.log(`[SQL] Saved encrypted session for ${job.number}; expires in 24 hours.`)
  return token
}

async function requestPairingCode(job) {
  if (job.cleaned || job.code || job.status === 'error' || job.codeRequesting || job.state?.creds?.registered) return
  job.codeRequesting = true
  try {
    await delay(2000)
    if (job.cleaned || job.code) return
    const rawCode = await job.sock.requestPairingCode(job.number)
    if (!rawCode) throw new Error('WhatsApp returned an empty pairing code')
    job.code = String(rawCode).replace(/[^A-Z0-9]/gi, '').match(/.{1,4}/g)?.join('-') || String(rawCode)
    job.status = 'waiting'
    job.error = ''
    console.log(`[PAIR] Pairing code generated for ${job.number}.`)
  } catch (error) {
    job.code = ''
    job.status = 'connecting'
    job.error = error?.message || 'Pairing code request failed'
    console.error('[PAIR] Pairing-code request failed:', job.error)
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

    if (!state.creds.registered && (connection === 'connecting' || qr) && !job.code && !job.codeRequesting) {
      await requestPairingCode(job)
    }
    if (connection === 'connecting' && !job.code) job.status = 'connecting'

    if (connection === 'open') {
      job.status = 'connected'
      job.error = ''
      try {
        await saveCreds()
        await delay(1500)
        if (job.cleaned) return

        // SQL commit completes before the token is sent to the user.
        const token = await saveSession(job)
        job.status = 'ready'
        const jid = `${job.number}@s.whatsapp.net`
        try {
          await sock.sendMessage(jid, { text: '✓ WhatsApp paired successfully.' })
          await delay(500)
          await sock.sendMessage(jid, { text: token })
          await delay(500)
          await sock.sendMessage(jid, {
            document: Buffer.from(token, 'utf8'),
            mimetype: 'text/plain',
            fileName: 'RAZA-SESSION.txt'
          })
          job.sent = true
        } catch (error) {
          job.sendError = error?.message || 'Could not send session messages'
          console.error('[PAIR] Could not send session details to WhatsApp:', job.sendError)
        }
        setTimeout(() => cleanupJob(job.id).catch(() => {}), 5000)
      } catch (error) {
        job.status = 'error'
        job.error = error?.message || 'Session generation or SQL save failed'
        console.error('[SQL] Could not persist session:', job.error)
        await cleanupJob(job.id)
      }
    }

    if (connection === 'close') {
      const statusCode = new Boom(lastDisconnect?.error)?.output?.statusCode
      console.log(`[PAIR] Temporary connection closed (${statusCode || 'unknown'}).`)
      if (statusCode === DisconnectReason.loggedOut) {
        job.status = 'error'
        job.error = 'WhatsApp logged out'
        await cleanupJob(job.id)
        return
      }
      if (isTerminal(job) || job.cleaned) return
      if (!job.reconnecting) {
        job.reconnecting = true
        setTimeout(async () => {
          try {
            if (job.cleaned) return
            job.reconnecting = false
            await connectJob(job)
          } catch (error) {
            job.reconnecting = false
            job.status = 'error'
            job.error = error?.message || 'Reconnect failed'
            await cleanupJob(job.id)
          }
        }, 2000)
      }
    }
  })
}

app.get('/health', async (req, res) => {
  try {
    await pool.query('SELECT 1')
    res.set('Cache-Control', 'no-store').json({ success: true, status: 'online', database: 'connected' })
  } catch {
    res.status(503).set('Cache-Control', 'no-store').json({ success: false, status: 'degraded', database: 'unavailable' })
  }
})

app.get('/', (req, res) => {
  res.sendFile(path.join(ROOT, 'public', 'index.html'))
})

app.post('/api/pair', async (req, res) => {
  try {
    const number = cleanNumber(req.body?.number)
    if (number.length < 7 || number.length > 15) {
      return res.status(400).json({ success: false, error: 'Invalid WhatsApp number. Include the country code.' })
    }

    for (const job of jobs.values()) {
      if (job.number === number && !isTerminal(job) && !job.cleaned) {
        return res.json({ success: true, id: job.id, code: job.code, status: job.status })
      }
    }

    const job = {
      id: makeId(),
      number,
      dir: path.join(SESSIONS, crypto.randomBytes(12).toString('hex')),
      code: '', token: '', status: 'starting', sent: false, error: '', sendError: '',
      codeRequesting: false, reconnecting: false, cleaned: false, sock: null, state: null,
      createdAt: Date.now()
    }
    await fs.mkdir(job.dir, { recursive: true })
    jobs.set(job.id, job)
    connectJob(job).catch(async error => {
      job.status = 'error'
      job.error = error?.message || 'Could not start pairing'
      console.error('[PAIR] Could not start pairing:', job.error)
      await cleanupJob(job.id)
    })

    for (let i = 0; i < 48; i++) {
      if (job.code || job.status === 'error') break
      await delay(250)
    }
    return res.json({ success: true, id: job.id, code: job.code, status: job.status })
  } catch (error) {
    console.error('[PAIR] Start error:', error?.message || error)
    return res.status(500).json({ success: false, error: 'Could not start pairing. Please try again.' })
  }
})

app.get('/api/status/:id', (req, res) => {
  res.set('Cache-Control', 'no-store')
  const job = jobs.get(req.params.id)
  if (!job) return res.status(404).json({ success: false, error: 'Pairing session not found' })
  return res.json({
    success: true, id: job.id, status: job.status, code: job.code,
    session: job.token, sent: job.sent, error: job.error, sendError: job.sendError
  })
})

app.get('/api/session/:token', async (req, res) => {
  res.set('Cache-Control', 'no-store, no-cache, must-revalidate, private')
  res.set('Pragma', 'no-cache')
  const token = String(req.params.token || '').trim()
  if (!/^RAZA_[A-Za-z0-9_-]{10,100}$/.test(token)) {
    return res.status(400).json({ success: false, error: 'Invalid session token' })
  }
  try {
    const [rows] = await pool.execute(
      `SELECT version, encrypted_payload FROM raza_pair_sessions
       WHERE token_hash = ? AND expires_at > UTC_TIMESTAMP(3) LIMIT 1`,
      [tokenHash(token)]
    )
    if (!rows.length) return res.status(404).json({ success: false, error: 'Session not found or expired' })
    const payload = decryptPayload(rows[0].encrypted_payload)
    if (!payload?.files || typeof payload.files !== 'object' || !payload.files['creds.json']) {
      console.error('[SQL] Stored session payload is incomplete.')
      return res.status(500).json({ success: false, error: 'Stored session is invalid' })
    }
    return res.json({ success: true, version: Number(rows[0].version), files: payload.files })
  } catch (error) {
    console.error('[SQL] Session retrieval failed:', error?.message || error)
    return res.status(500).json({ success: false, error: 'Could not retrieve session' })
  }
})

/*
 * The deployed bot uploads updated Baileys auth files after credential changes
 * and periodically while running. This keeps the SQL snapshot current across
 * Heroku dyno restarts. The session token is the bearer credential for this API.
 */
app.put('/api/session/:token', async (req, res) => {
  res.set('Cache-Control', 'no-store, no-cache, must-revalidate, private')
  const token = String(req.params.token || '').trim()
  if (!/^RAZA_[A-Za-z0-9_-]{10,100}$/.test(token)) {
    return res.status(400).json({ success: false, error: 'Invalid session token' })
  }

  const files = req.body?.files
  const version = Number(req.body?.version || 1)
  if (!files || typeof files !== 'object' || Array.isArray(files) || !files['creds.json']) {
    return res.status(400).json({ success: false, error: 'Session payload must include files.creds.json' })
  }

  const safeFiles = {}
  let totalChars = 0
  for (const [name, encoded] of Object.entries(files)) {
    if (!/^[A-Za-z0-9._-]+$/.test(name) || name === '.' || name === '..') {
      return res.status(400).json({ success: false, error: 'Invalid authentication filename' })
    }
    if (typeof encoded !== 'string' || !/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(encoded)) {
      return res.status(400).json({ success: false, error: 'Invalid Base64 authentication file' })
    }
    totalChars += encoded.length
    if (totalChars > 20 * 1024 * 1024) {
      return res.status(413).json({ success: false, error: 'Session payload is too large' })
    }
    safeFiles[name] = encoded
  }

  try {
    const encrypted = encryptPayload({ version: 1, files: safeFiles })
    const expiresAt = new Date(Date.now() + SESSION_TTL)
    const [result] = await pool.execute(
      `UPDATE raza_pair_sessions
       SET version = ?, encrypted_payload = ?, expires_at = ?
       WHERE token_hash = ?`,
      [Number.isFinite(version) ? version : 1, encrypted, expiresAt, tokenHash(token)]
    )
    if (!result.affectedRows) {
      return res.status(404).json({ success: false, error: 'Session token not found; pair again to create a new session' })
    }
    return res.json({ success: true, updated: true })
  } catch (error) {
    console.error('[SQL] Session update failed:', error?.message || error)
    return res.status(500).json({ success: false, error: 'Could not update session' })
  }
})

setInterval(async () => {
  try {
    const [result] = await pool.execute('DELETE FROM raza_pair_sessions WHERE expires_at <= UTC_TIMESTAMP(3)')
    if (result.affectedRows) console.log(`[SQL] Removed ${result.affectedRows} expired session(s).`)
  } catch (error) {
    console.error('[SQL] Expiry cleanup failed:', error?.message || error)
  }

  const now = Date.now()
  for (const [jobId, job] of jobs) {
    if (now - job.createdAt >= JOB_TTL) {
      await cleanupJob(jobId)
      jobs.delete(jobId)
    }
  }
  for (const [ip, timestamps] of hits) {
    const fresh = timestamps.filter(t => now - t < 10 * 60 * 1000)
    if (fresh.length) hits.set(ip, fresh)
    else hits.delete(ip)
  }
}, 60 * 1000).unref()

mountDeploy(app)

const server = app.listen(PORT, () => {
  console.log(`RAZA Pair & Deploy running on port ${PORT}`)
  console.log('[SQL] Encrypted session storage enabled; bot snapshots refresh the 30-day expiry.')
})

async function shutdown(signal) {
  console.log(`[PROCESS] ${signal} received; shutting down.`)
  server.close()
  for (const job of jobs.values()) await cleanupJob(job.id)
  try { await pool.end() } catch {}
  process.exit(0)
}
process.on('SIGTERM', () => shutdown('SIGTERM'))
process.on('SIGINT', () => shutdown('SIGINT'))
