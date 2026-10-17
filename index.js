import express from 'express'
import P from 'pino'
import crypto from 'node:crypto'
import fs from 'node:fs/promises'
import path from 'node:path'

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

const SESSION_TTL = 24 * 60 * 60 * 1000
const JOB_TTL = 10 * 60 * 1000

const logger = P({ level: 'silent' })

const jobs = new Map()
const storedSessions = new Map()

app.use(express.json({ limit: '2mb' }))
app.use(express.static(path.join(ROOT, 'public')))

await fs.mkdir(SESSIONS, { recursive: true })

function makeId() {
  return crypto.randomBytes(16).toString('hex')
}

function makeToken() {
  return 'RAZA_' + crypto.randomBytes(18).toString('base64url')
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

/*
 * Clean up temporary WhatsApp pairing connections.
 */
async function cleanupJob(jobId) {
  const job = jobs.get(jobId)

  if (!job || job.cleaned) return

  job.cleaned = true

  try {
    if (job.sock) {
      try {
        job.sock.ev.removeAllListeners('connection.update')
      } catch {}

      try {
        job.sock.ev.removeAllListeners('creds.update')
      } catch {}

      try {
        job.sock.ws?.close()
      } catch {}

      try {
        job.sock.end(undefined)
      } catch {}
    }
  } catch {}

  try {
    await fs.rm(job.dir, {
      recursive: true,
      force: true
    })
  } catch {}
}

/*
 * Convert Baileys authentication files to Base64.
 */
async function encodeSession(authDir) {
  const files = {}
  const names = await fs.readdir(authDir)

  for (const name of names) {
    const file = path.join(authDir, name)
    const stat = await fs.stat(file)

    if (stat.isFile()) {
      files[name] = (
        await fs.readFile(file)
      ).toString('base64')
    }
  }

  return {
    version: 1,
    files
  }
}

/*
 * Store the authentication files under a random token.
 */
async function saveSession(job) {
  const data = await encodeSession(job.dir)
  const token = makeToken()

  storedSessions.set(token, {
    token,
    number: job.number,
    version: data.version,
    files: data.files,
    createdAt: Date.now()
  })

  job.token = token

  return token
}

/*
 * Request a WhatsApp pairing code.
 */
async function requestPairingCode(job) {
  if (
    job.cleaned ||
    job.code ||
    job.status === 'error' ||
    job.codeRequesting ||
    job.state?.creds?.registered
  ) {
    return
  }

  job.codeRequesting = true

  try {
    await delay(2000)

    if (job.cleaned || job.code) return

    const rawCode = await job.sock.requestPairingCode(
      job.number
    )

    if (!rawCode) {
      throw new Error(
        'WhatsApp returned an empty pairing code'
      )
    }

    job.code =
      String(rawCode)
        .replace(/[^A-Z0-9]/gi, '')
        .match(/.{1,4}/g)
        ?.join('-') || String(rawCode)

    job.status = 'waiting'
    job.error = ''
  } catch (error) {
    job.code = ''
    job.status = 'connecting'
    job.error =
      error?.message || 'Pairing code request failed'
  } finally {
    job.codeRequesting = false
  }
}

/*
 * Connect a temporary Baileys socket for pairing.
 */
async function connectJob(job) {
  if (job.cleaned) return

  const { state, saveCreds } = await useMultiFileAuthState(
    job.dir
  )

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

    /*
     * Ask for a pairing code before the account is registered.
     */
    if (
      !state.creds.registered &&
      (connection === 'connecting' || qr) &&
      !job.code &&
      !job.codeRequesting
    ) {
      await requestPairingCode(job)
    }

    if (connection === 'connecting' && !job.code) {
      job.status = 'connecting'
    }

    /*
     * SUCCESS: WhatsApp has connected.
     */
    if (connection === 'open') {
      job.status = 'connected'
      job.error = ''

      try {
        await saveCreds()
        await delay(1500)

        if (job.cleaned) return

        const token = await saveSession(job)

        job.status = 'ready'

        const jid = `${job.number}@s.whatsapp.net`

        /*
         * Send confirmation and session information.
         */
        try {
          await sock.sendMessage(jid, {
            text: '✓ WhatsApp paired successfully.'
          })

          await delay(500)

          await sock.sendMessage(jid, {
            text: token
          })

          await delay(500)

          await sock.sendMessage(jid, {
            document: Buffer.from(token, 'utf8'),
            mimetype: 'text/plain',
            fileName: 'RAZA-SESSION.txt'
          })

          job.sent = true
        } catch (error) {
          job.sendError =
            error?.message ||
            'Could not send session messages'
        }

        /*
         * Keep the token in memory.
         * Close only the temporary pairing socket.
         */
        setTimeout(() => {
          cleanupJob(job.id).catch(() => {})
        }, 5000)
      } catch (error) {
        job.status = 'error'
        job.error =
          error?.message || 'Session generation failed'

        await cleanupJob(job.id)
      }
    }

    /*
     * CONNECTION CLOSED.
     */
    if (connection === 'close') {
      const statusCode = new Boom(
        lastDisconnect?.error
      )?.output?.statusCode

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
            job.error =
              error?.message || 'Reconnect failed'

            await cleanupJob(job.id)
          }
        }, 2000)
      }
    }
  })
}

/*
 * HEALTH CHECK
 */
app.get('/health', (req, res) => {
  res.set('Cache-Control', 'no-store')

  res.json({
    success: true,
    status: 'online'
  })
})

/*
 * HOME PAGE
 */
app.get('/', (req, res) => {
  res.sendFile(
    path.join(ROOT, 'public', 'index.html')
  )
})

/*
 * START WHATSAPP PAIRING
 */
app.post('/api/pair', async (req, res) => {
  try {
    const number = cleanNumber(req.body?.number)

    if (number.length < 7 || number.length > 15) {
      return res.status(400).json({
        success: false,
        error: 'Invalid WhatsApp number. Include the country code.'
      })
    }

    /*
     * Reuse an active pairing job for this number.
     */
    for (const job of jobs.values()) {
      if (
        job.number === number &&
        !isTerminal(job) &&
        !job.cleaned
      ) {
        return res.json({
          success: true,
          id: job.id,
          code: job.code,
          status: job.status
        })
      }
    }

    const job = {
      id: makeId(),
      number,
      dir: path.join(
        SESSIONS,
        crypto.randomBytes(12).toString('hex')
      ),
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

    connectJob(job).catch(async error => {
      job.status = 'error'
      job.error =
        error?.message || 'Could not start pairing'

      await cleanupJob(job.id)
    })

    /*
     * Wait up to 12 seconds for a code or an error.
     */
    for (let i = 0; i < 48; i++) {
      if (job.code || job.status === 'error') break
      await delay(250)
    }

    return res.json({
      success: true,
      id: job.id,
      code: job.code,
      status: job.status
    })
  } catch (error) {
    return res.status(500).json({
      success: false,
      error: error?.message || 'Internal server error'
    })
  }
})

/*
 * PAIRING STATUS
 */
app.get('/api/status/:id', (req, res) => {
  res.set('Cache-Control', 'no-store')

  const job = jobs.get(req.params.id)

  if (!job) {
    return res.status(404).json({
      success: false,
      error: 'Pairing session not found'
    })
  }

  return res.json({
    success: true,
    id: job.id,
    status: job.status,
    code: job.code,
    session: job.token,
    sent: job.sent,
    error: job.error,
    sendError: job.sendError
  })
})

/*
 * SESSION DOWNLOAD API
 *
 * The deployed bot calls this endpoint using SESSION_ID.
 * Treat the token like a password: it grants access to
 * the WhatsApp authentication files.
 */
app.get('/api/session/:token', (req, res) => {
  res.set('Cache-Control', 'no-store')
  res.set('Pragma', 'no-cache')

  const token = String(req.params.token || '').trim()

  if (!/^RAZA_[A-Za-z0-9_-]{10,100}$/.test(token)) {
    return res.status(400).json({
      success: false,
      error: 'Invalid session token'
    })
  }

  const session = storedSessions.get(token)

  if (!session) {
    return res.status(404).json({
      success: false,
      error: 'Session not found or expired'
    })
  }

  return res.json({
    success: true,
    version: session.version,
    files: session.files
  })
})

/*
 * REMOVE EXPIRED SESSIONS AND OLD PAIRING JOBS.
 */
setInterval(async () => {
  const now = Date.now()

  for (const [token, session] of storedSessions) {
    if (now - session.createdAt >= SESSION_TTL) {
      storedSessions.delete(token)
    }
  }

  for (const [jobId, job] of jobs) {
    if (now - job.createdAt >= JOB_TTL) {
      await cleanupJob(jobId)
      jobs.delete(jobId)
    }
  }
}, 60 * 1000).unref()

/*
 * MOUNT HEROKU DEPLOYMENT ROUTES.
 *
 * deploy.js provides:
 * POST /api/deploy
 * GET  /api/deploy/:id
 */
mountDeploy(app)

/*
 * START SERVER.
 */
app.listen(PORT, () => {
  console.log(`RAZA Pair & Deploy running on port ${PORT}`)
})
