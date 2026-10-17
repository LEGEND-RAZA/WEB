import crypto from 'node:crypto'

/*
 * One-tap Heroku deploy for RAZA MD.
 * Portal sessions are SQL-backed; this module validates them through the
 * portal's own API before creating a bot app. The token is never logged.
 */
const API = 'https://api.heroku.com'
const FORWARD = ['PREFIX', 'DB_HOST', 'DB_PORT', 'DB_NAME', 'DB_USER', 'DB_PASSWORD', 'DB_SSL']
const jobs = new Map()
const used = new Map()
const hits = new Map()
const delay = ms => new Promise(resolve => setTimeout(resolve, ms))

async function heroku(method, route, body) {
  const res = await fetch(API + route, {
    method,
    headers: {
      Accept: 'application/vnd.heroku+json; version=3',
      Authorization: `Bearer ${process.env.HEROKU_API_KEY}`,
      'Content-Type': 'application/json'
    },
    body: body ? JSON.stringify(body) : undefined,
    signal: AbortSignal.timeout(30000)
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

async function sessionExists(token) {
  try {
    const base = `http://127.0.0.1:${process.env.PORT || 3000}`
    const r = await fetch(`${base}/api/session/${encodeURIComponent(token)}`, {
      signal: AbortSignal.timeout(10000),
      headers: { Accept: 'application/json' }
    })
    if (!r.ok) return false
    const data = await r.json()
    return data?.success === true && data?.files && typeof data.files === 'object' && Boolean(data.files['creds.json'])
  } catch {
    return false
  }
}

async function run(job, name, token) {
  const repo = process.env.BOT_REPO || 'LEGEND-RAZA/RAZA-MD'
  const branch = process.env.BOT_BRANCH || 'main'
  const size = process.env.DYNO_SIZE || 'basic'
  let appCreated = false

  try {
    job.status = 'creating'
    const body = { name, stack: 'heroku-24' }
    if (process.env.HEROKU_REGION) body.region = process.env.HEROKU_REGION
    const herokuApp = process.env.HEROKU_TEAM
      ? await heroku('POST', '/teams/apps', { ...body, team: process.env.HEROKU_TEAM })
      : await heroku('POST', '/apps', body)
    appCreated = true
    job.created = true

    job.status = 'config'
    const vars = {
      SESSION_ID: token,
      PAIR_SERVER_URL: (process.env.PAIR_SERVER_URL || '').replace(/\/+$/, ''),
      HEROKU_APP_NAME: herokuApp.name || name,
      PREFIX: process.env.PREFIX || '.'
    }
    if (!vars.PAIR_SERVER_URL) throw new Error('PAIR_SERVER_URL must be set to this portal’s public HTTPS URL.')
    for (const key of FORWARD) if (process.env[key]) vars[key] = process.env[key]
    if (process.env.PASS_HEROKU_KEY === 'true') vars.HEROKU_API_KEY = process.env.HEROKU_API_KEY
    await heroku('PATCH', `/apps/${name}/config-vars`, vars)

    job.status = 'building'
    const build = await heroku('POST', `/apps/${name}/builds`, {
      source_blob: {
        url: `https://github.com/${repo}/archive/refs/heads/${branch}.tar.gz`,
        version_description: `Raza-MD deploy from ${repo}@${branch}`
      }
    })
    let buildFinished = false
    for (let i = 0; i < 120; i++) {
      await delay(5000)
      const result = await heroku('GET', `/apps/${name}/builds/${build.id}`)
      if (result.status === 'succeeded') { buildFinished = true; break }
      if (result.status === 'failed') throw new Error('The build failed. Check the bot repository build logs.')
    }
    if (!buildFinished) throw new Error('The build took too long.')

    job.status = 'starting'
    const formations = await heroku('GET', `/apps/${name}/formation`)
    const types = formations.map(f => f.type)
    if (!types.includes('worker')) throw new Error('The bot repository has no worker process in its Procfile.')
    await heroku('PATCH', `/apps/${name}/formation`, {
      updates: [
        ...(types.includes('web') ? [{ type: 'web', quantity: 0, size }] : []),
        { type: 'worker', quantity: 1, size }
      ]
    })
    job.status = 'live'
  } catch (error) {
    job.error = error?.message || 'Deployment failed.'
    job.status = 'failed'
    used.delete(token)
    if (appCreated) {
      try { await heroku('DELETE', `/apps/${name}`) } catch {}
    }
  }
}

export function mountDeploy(app) {
  app.post('/api/deploy', async (req, res) => {
    const fail = (code, error) => res.status(code).json({ success: false, error })
    if (!process.env.HEROKU_API_KEY) return fail(503, 'Deployment is not configured yet.')
    if (!process.env.PAIR_SERVER_URL) return fail(503, 'PAIR_SERVER_URL is not configured on the portal.')
    if (limited(req.ip)) return fail(429, 'Too many attempts. Try again in a few minutes.')

    const name = String(req.body?.appName || '').trim().toLowerCase()
    const token = String(req.body?.sessionId || '').trim()
    if (!/^[a-z][a-z0-9-]{2,29}$/.test(name)) return fail(400, 'Invalid app name.')
    if (!/^RAZA_[A-Za-z0-9_-]{10,100}$/.test(token) || !(await sessionExists(token))) {
      return fail(400, 'Session ID not found, expired, or unavailable. Pair again and use the new token.')
    }
    if (used.has(token)) return fail(409, 'This session ID is already being deployed.')

    const active = [...jobs.values()].filter(j => !['live', 'failed'].includes(j.status)).length
    if (active >= (Number(process.env.MAX_ACTIVE) || 3)) return fail(429, 'The deployer is busy. Try again in a minute.')

    const job = { id: crypto.randomBytes(12).toString('hex'), status: 'creating', error: '', createdAt: Date.now() }
    jobs.set(job.id, job)
    used.set(token, name)
    void run(job, name, token)
    return res.json({ success: true, id: job.id })
  })

  app.get('/api/deploy/:id', (req, res) => {
    const job = jobs.get(req.params.id)
    if (!job) return res.status(404).json({ success: false, error: 'Deployment not found.' })
    return res.json({ success: true, status: job.status, error: job.error })
  })

  setInterval(() => {
    const now = Date.now()
    for (const [id, job] of jobs) if (now - job.createdAt > 60 * 60 * 1000) jobs.delete(id)
    for (const [ip, timestamps] of hits) {
      const fresh = timestamps.filter(t => now - t < 10 * 60 * 1000)
      if (fresh.length) hits.set(ip, fresh)
      else hits.delete(ip)
    }
  }, 10 * 60 * 1000).unref()
}
