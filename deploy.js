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
 *   HEROKU_TEAM, HEROKU_REGION, MAX_ACTIVE   optional
 */

const API = 'https://api.heroku.com'
const FORWARD = ['PREFIX', 'DB_HOST', 'DB_PORT', 'DB_NAME', 'DB_USER', 'DB_PASSWORD']
const jobs = new Map()
const used = new Map()
const hits = new Map()
const delay = ms => new Promise(r => setTimeout(r, ms))
const safe = fn => (req, res) =>
  Promise.resolve(fn(req, res)).catch(e => {
    console.error('[DEPLOY]', e?.message || e)
    if (!res.headersSent) res.status(500).json({ success: false, error: 'Server error. Try again.' })
  })

async function heroku(method, route, body) {
  const res = await fetch(API + route, {
    method,
    headers: {
      Accept: 'application/vnd.heroku+json; version=3',
      Authorization: `Bearer ${process.env.HEROKU_API_KEY}`,
      'Content-Type': 'application/json'
    },
    body: body ? JSON.stringify(body) : undefined
  })
  const data = await res.json().catch(() => ({}))
  if (!res.ok) throw new Error(data.message || `Heroku error ${res.status}`)
  return data
}

function limited(ip) {
  const now = Date.now()
  const list = (hits.get(ip) || []).filter(t => now - t < 10 * 60 * 1000)
  list.push(now)
  hits.set(ip, list)
  return list.length > 5
}

const sessionOk = token => sessionExists(token).catch(() => false)

async function run(job, name, token) {
  const repo = process.env.BOT_REPO || 'LEGEND-RAZA/RAZA-MD'
  const branch = process.env.BOT_BRANCH || 'main'
  const size = process.env.DYNO_SIZE || 'basic'

  try {
    job.status = 'creating'
    const body = { name, stack: 'heroku-24' }
    if (process.env.HEROKU_REGION) body.region = process.env.HEROKU_REGION
    const app = process.env.HEROKU_TEAM
      ? await heroku('POST', '/teams/apps', { ...body, team: process.env.HEROKU_TEAM })
      : await heroku('POST', '/apps', body)
    job.created = true

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
    for (let i = 0; ; i++) {
      await delay(5000)
      const b = await heroku('GET', `/apps/${name}/builds/${build.id}`)
      if (b.status === 'succeeded') break
      if (b.status === 'failed') throw new Error('The build failed. Please try again in a few minutes.')
      if (i >= 119) throw new Error('The build took too long.')
    }

    // Worker only: the process types exist once the first release is out.
    job.status = 'starting'
    const types = (await heroku('GET', `/apps/${name}/formation`)).map(f => f.type)
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
    used.delete(token)
    if (job.created) heroku('DELETE', `/apps/${name}`).catch(() => {})
  }
}

export function mountDeploy(app) {
  app.set('trust proxy', 1)

  app.post('/api/deploy', safe(async (req, res) => {
    const fail = (code, error) => res.status(code).json({ success: false, error })

    if (!process.env.HEROKU_API_KEY) return fail(503, 'Deployment is not configured yet.')
    if (limited(req.ip)) return fail(429, 'Too many attempts. Try again in a few minutes.')

    const name = String(req.body?.appName || '').trim().toLowerCase()
    const token = String(req.body?.sessionId || '').trim()

    if (!/^[a-z][a-z0-9-]{2,29}$/.test(name)) return fail(400, 'Invalid app name.')
    if (!/^RAZA_[A-Za-z0-9_-]{10,100}$/.test(token) || !(await sessionOk(token))) {
      return fail(400, 'Session ID not found or expired. Get a new one.')
    }
    if (used.has(token)) return fail(409, 'This session ID is already being deployed.')

    const active = [...jobs.values()].filter(j => !['live', 'failed'].includes(j.status)).length
    if (active >= (Number(process.env.MAX_ACTIVE) || 3)) {
      return fail(429, 'The deployer is busy. Try again in a minute.')
    }

    const job = {
      id: crypto.randomBytes(12).toString('hex'),
      status: 'creating',
      error: '',
      createdAt: Date.now()
    }
    jobs.set(job.id, job)
    used.set(token, name)
    run(job, name, token)

    res.json({ success: true, id: job.id })
  }))

  app.get('/api/deploy/:id', (req, res) => {
    const job = jobs.get(req.params.id)
    if (!job) return res.status(404).json({ success: false, error: 'Deployment not found.' })
    res.json({ success: true, status: job.status, error: job.error })
  })

  setInterval(() => {
    for (const [id, j] of jobs) {
      if (Date.now() - j.createdAt > 60 * 60 * 1000) jobs.delete(id)
    }
  }, 10 * 60 * 1000).unref()
}
