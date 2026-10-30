import crypto from 'node:crypto'
import { sessionExists } from './db.js'

/*
 * One-tap Heroku deploy for RAZA MD, same flow as the .deploy plugin.
 * The bot runs as a worker only: web is scaled to 0 and worker to 1.
 * Uses Node's built-in fetch (Node 18+), so no new packages are needed.
 *
 * Config vars on THIS web app:
 *   HEROKU_API_KEY   required  (auto-forwarded to every bot as HEROKU_API_KEY)
 *   PAIR_SERVER_URL  this site's public URL (each bot fetches its session from it)
 *   BOT_REPO         default LEGEND-RAZA/RAZA-MD
 *   BOT_BRANCH       default main
 *   DYNO_SIZE        default basic
 *   PREFIX, DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
 *                    passed to every bot when set
 *   PASS_HEROKU_KEY  "false" to DISABLE forwarding HEROKU_API_KEY (default: forwards)
 *   AUTO_DEPLOY      "false" to DISABLE auto-deploy right after pairing (default: on)
 *   HEROKU_TEAM, HEROKU_REGION, MAX_ACTIVE   optional
 *
 * Deploys are idempotent per session: calling deploy again for the same session
 * (web page, .pair plugin, or the auto-deploy after pairing) attaches to the
 * existing job instead of creating a second bot on the same WhatsApp session.
 */

const API = 'https://api.heroku.com'
const FORWARD = ['PREFIX', 'DB_HOST', 'DB_PORT', 'DB_NAME', 'DB_USER', 'DB_PASSWORD']
const NAME_RE = /^[a-z][a-z0-9-]{2,29}$/
const TOKEN_RE = /^RAZA_[A-Za-z0-9_-]{10,100}$/

const RATE_WINDOW = 10 * 60 * 1000
const JOB_TTL = 60 * 60 * 1000
const USED_TTL = 7 * 24 * 60 * 60 * 1000

const jobs = new Map()
const used = new Map()   // token -> { name, jobId, at }
const hits = new Map()
const delay = ms => new Promise(r => setTimeout(r, ms))

class DeployError extends Error {
  constructor(code, message) {
    super(message)
    this.code = code
  }
}

const safe = fn => (req, res) =>
  Promise.resolve(fn(req, res)).catch(e => {
    console.error('[DEPLOY]', e?.message || e)
    if (!res.headersSent) res.status(500).json({ success: false, error: 'Server error. Try again.' })
  })

const makeName = () => `raza-${crypto.randomBytes(3).toString('hex')}`

async function heroku(method, route, body) {
  const res = await fetch(API + route, {
    method,
    headers: {
      Accept: 'application/vnd.heroku+json; version=3',
      Authorization: `Bearer ${process.env.HEROKU_API_KEY}`,
      'Content-Type': 'application/json'
    },
    body: body ? JSON.stringify(body) : undefined,
    signal: AbortSignal.timeout(30000)   // a hung Heroku call can no longer freeze a deploy
  })
  const data = await res.json().catch(() => ({}))
  if (!res.ok) throw new Error(data.message || `Heroku error ${res.status}`)
  return data
}

function limited(ip) {
  const now = Date.now()
  const list = (hits.get(ip) || []).filter(t => now - t < RATE_WINDOW)
  list.push(now)
  hits.set(ip, list)
  return list.length > 5
}

const sessionOk = token => sessionExists(token).catch(() => false)

// Create the Heroku app, retrying with a fresh name if the random one is already taken.
async function createApp(base) {
  let name = base
  for (let attempt = 0; ; attempt++) {
    try {
      const body = { name, stack: 'heroku-24' }
      if (process.env.HEROKU_REGION) body.region = process.env.HEROKU_REGION
      const app = process.env.HEROKU_TEAM
        ? await heroku('POST', '/teams/apps', { ...body, team: process.env.HEROKU_TEAM })
        : await heroku('POST', '/apps', body)
      return { app, name }
    } catch (e) {
      if (attempt < 3 && /already taken/i.test(e.message || '')) {
        name = makeName()
        continue
      }
      throw e
    }
  }
}

async function run(job, baseName, token) {
  const repo = process.env.BOT_REPO || 'LEGEND-RAZA/RAZA-MD'
  const branch = process.env.BOT_BRANCH || 'main'
  const size = process.env.DYNO_SIZE || 'basic'
  let name = baseName

  try {
    job.status = 'creating'
    const made = await createApp(baseName)
    const app = made.app
    name = made.name
    job.appName = name
    job.created = true
    const entry = used.get(token)
    if (entry && entry.jobId === job.id) entry.name = name

    job.status = 'config'
    const vars = {
      SESSION_ID: token,
      PAIR_SERVER_URL: process.env.PAIR_SERVER_URL || 'https://pair-web-3e08f4e68faf.herokuapp.com',
      HEROKU_APP_NAME: app.name || name,
      PREFIX: '.'
    }
    for (const k of FORWARD) if (process.env[k]) vars[k] = process.env[k]

    // Auto-forward this web app's Heroku key to the bot unless explicitly disabled.
    // Opt-out:  heroku config:set PASS_HEROKU_KEY=false -a <this-web-app>
    if (process.env.PASS_HEROKU_KEY !== 'false' && process.env.HEROKU_API_KEY) {
      vars.HEROKU_API_KEY = process.env.HEROKU_API_KEY
    }

    await heroku('PATCH', `/apps/${name}/config-vars`, vars)

    job.status = 'building'
    const build = await heroku('POST', `/apps/${name}/builds`, {
      source_blob: {
        url: `https://github.com/${repo}/archive/refs/heads/${branch}.tar.gz`,
        version_description: `Raza-MD deploy from ${repo}@${branch}`
      }
    })

    // Poll the build. A single network blip must NOT kill the deploy and delete the app,
    // so tolerate a few consecutive poll errors.
    let pollErrors = 0
    for (let i = 0; ; i++) {
      await delay(5000)
      let b
      try {
        b = await heroku('GET', `/apps/${name}/builds/${build.id}`)
        pollErrors = 0
      } catch (e) {
        if (++pollErrors >= 5) throw e
        continue
      }
      if (b.status === 'succeeded') break
      if (b.status === 'failed') throw new Error('The build failed. Please try again in a few minutes.')
      if (i >= 119) throw new Error('The build took too long.')
    }

    // Worker only. The process types can appear a few seconds after the build
    // succeeds (the release is created asynchronously), so wait for them.
    job.status = 'starting'
    let types = []
    for (let i = 0; i < 12; i++) {
      try {
        types = (await heroku('GET', `/apps/${name}/formation`)).map(f => f.type)
      } catch (e) {
        if (i >= 11) throw e
      }
      if (types.includes('worker')) break
      await delay(5000)
    }
    if (!types.includes('worker')) throw new Error('The bot has no worker process in its Procfile.')

    await heroku('PATCH', `/apps/${name}/formation`, {
      updates: [
        ...(types.includes('web') ? [{ type: 'web', quantity: 0, dyno_size: { name: size } }] : []),
        { type: 'worker', quantity: 1, dyno_size: { name: size } }
      ]
    })

    job.status = 'live'
  } catch (e) {
    job.error = e.message || 'Deployment failed.'
    job.status = 'failed'
    const entry = used.get(token)
    if (entry && entry.jobId === job.id) used.delete(token)   // allow a retry
    if (job.created) heroku('DELETE', `/apps/${name}`).catch(() => {})
  }
}

/*
 * Start (or attach to) the deployment for a session.
 * Used by POST /api/deploy and by the pair server's auto-deploy.
 * Returns { id, existing }. Throws DeployError with an HTTP-style code.
 */
export async function startDeploy(token, requestedName) {
  if (!process.env.HEROKU_API_KEY) throw new DeployError(503, 'Deployment is not configured yet.')

  token = String(token || '').trim()
  const name = String(requestedName || makeName()).trim().toLowerCase()

  if (!NAME_RE.test(name)) throw new DeployError(400, 'Invalid app name.')
  if (!TOKEN_RE.test(token) || !(await sessionOk(token))) {
    throw new DeployError(400, 'Session ID not found or expired. Get a new one.')
  }

  // No awaits between this check and used.set(), so two concurrent calls can't both start a deploy.
  const prior = used.get(token)
  if (prior) {
    if (jobs.has(prior.jobId)) return { id: prior.jobId, existing: true }
    throw new DeployError(409, `This session is already deployed as ${prior.name}.`)
  }

  const active = [...jobs.values()].filter(j => !['live', 'failed'].includes(j.status)).length
  if (active >= (Number(process.env.MAX_ACTIVE) || 3)) {
    throw new DeployError(429, 'The deployer is busy. Try again in a minute.')
  }

  const job = {
    id: crypto.randomBytes(12).toString('hex'),
    status: 'creating',
    error: '',
    appName: name,
    created: false,
    createdAt: Date.now()
  }
  jobs.set(job.id, job)
  used.set(token, { name, jobId: job.id, at: Date.now() })
  run(job, name, token)

  return { id: job.id, existing: false }
}

export function getDeployStatus(id) {
  const job = jobs.get(id)
  if (!job) return { status: 'unknown', error: 'Deployment not found.', appName: '' }
  return { status: job.status, error: job.error, appName: job.appName }
}

export function mountDeploy(app) {
  app.set('trust proxy', 1)

  app.post('/api/deploy', safe(async (req, res) => {
    const fail = (code, error) => res.status(code).json({ success: false, error })

    if (!process.env.HEROKU_API_KEY) return fail(503, 'Deployment is not configured yet.')
    if (limited(req.ip)) return fail(429, 'Too many attempts. Try again in a few minutes.')

    try {
      const r = await startDeploy(req.body?.sessionId, req.body?.appName)
      res.json({ success: true, id: r.id, existing: r.existing })
    } catch (e) {
      if (e instanceof DeployError) return fail(e.code, e.message)
      throw e
    }
  }))

  app.get('/api/deploy/:id', (req, res) => {
    const job = jobs.get(req.params.id)
    if (!job) return res.status(404).json({ success: false, error: 'Deployment not found.' })
    res.set('Cache-Control', 'no-store')
    res.json({ success: true, status: job.status, error: job.error, appName: job.appName })
  })

  setInterval(() => {
    const now = Date.now()
    for (const [id, j] of jobs) if (now - j.createdAt > JOB_TTL) jobs.delete(id)
    for (const [t, u] of used) if (now - u.at > USED_TTL) used.delete(t)
    for (const [ip, list] of hits) {
      const fresh = list.filter(t => now - t < RATE_WINDOW)
      if (fresh.length) hits.set(ip, fresh)
      else hits.delete(ip)
    }
  }, 10 * 60 * 1000).unref()
}
