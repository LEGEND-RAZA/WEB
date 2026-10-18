import crypto from 'node:crypto'
import mysql from 'mysql2/promise'

/*
 * MySQL session storage.
 * Uses the same DB_* variables as the bot. The table is created automatically.
 * Optional: set SESSION_SECRET to encrypt sessions at rest (set it once, never change it).
 */

const secret = process.env.SESSION_SECRET
const key = secret ? crypto.createHash('sha256').update(secret).digest() : null

function seal(text) {
  if (!key) return text
  const iv = crypto.randomBytes(12)
  const c = crypto.createCipheriv('aes-256-gcm', key, iv)
  const data = Buffer.concat([c.update(text, 'utf8'), c.final()])
  return ['enc1', iv.toString('base64'), c.getAuthTag().toString('base64'), data.toString('base64')].join(':')
}

function unseal(text) {
  if (!text.startsWith('enc1:')) return text
  if (!key) throw new Error('Session is encrypted but SESSION_SECRET is not set')
  const [, iv, tag, data] = text.split(':')
  const d = crypto.createDecipheriv('aes-256-gcm', key, Buffer.from(iv, 'base64'))
  d.setAuthTag(Buffer.from(tag, 'base64'))
  return Buffer.concat([d.update(Buffer.from(data, 'base64')), d.final()]).toString('utf8')
}

let pool = null
function db() {
  if (!pool) {
    const { DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, DB_SSL } = process.env
    if (!DB_HOST || !DB_USER) throw new Error('Database is not configured')
    pool = mysql.createPool({
      host: DB_HOST,
      port: Number(DB_PORT) || 3306,
      database: DB_NAME,
      user: DB_USER,
      password: DB_PASSWORD,
      ssl: DB_SSL === 'false' ? undefined : { rejectUnauthorized: false },
      connectionLimit: 5,
      waitForConnections: true,
      connectTimeout: 15000,
      enableKeepAlive: true,
      keepAliveInitialDelay: 10000
    })
  }
  return pool
}

// One retry if MySQL dropped an idle connection.
async function exec(sql, params) {
  try {
    return await db().execute(sql, params)
  } catch (e) {
    if (['PROTOCOL_CONNECTION_LOST', 'ECONNRESET', 'EPIPE'].includes(e?.code)) return db().execute(sql, params)
    throw e
  }
}

async function initDb() {
  await db().query(`CREATE TABLE IF NOT EXISTS raza_sessions (
    token VARCHAR(64) NOT NULL PRIMARY KEY,
    number VARCHAR(20) NOT NULL,
    version INT NOT NULL DEFAULT 1,
    files LONGTEXT NOT NULL,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    KEY idx_number (number)
  ) DEFAULT CHARSET=utf8mb4`)
}

let ready = null
export function ensureDb() {
  if (!ready) ready = initDb().catch(e => { ready = null; throw e })
  return ready
}

export async function putSession({ token, number, version, files }) {
  await ensureDb()
  const now = Date.now()
  await exec(
    `INSERT INTO raza_sessions (token, number, version, files, created_at, updated_at)
     VALUES (?, ?, ?, ?, ?, ?)
     ON DUPLICATE KEY UPDATE version = VALUES(version), files = VALUES(files), updated_at = VALUES(updated_at)`,
    [token, number, version, seal(JSON.stringify(files)), now, now]
  )
}

export async function getSession(token) {
  await ensureDb()
  const [rows] = await exec('SELECT version, files FROM raza_sessions WHERE token = ? LIMIT 1', [token])
  if (!rows.length) return null
  return { version: rows[0].version, files: JSON.parse(unseal(rows[0].files)) }
}

export async function sessionExists(token) {
  await ensureDb()
  const [rows] = await exec('SELECT 1 FROM raza_sessions WHERE token = ? LIMIT 1', [token])
  return rows.length > 0
}

export async function pruneSessions(days) {
  await ensureDb()
  await exec('DELETE FROM raza_sessions WHERE created_at < ?', [Date.now() - days * 86400000])
}

export async function updateSession(token, files) {
  await ensureDb()
  const [result] = await exec(
    'UPDATE raza_sessions SET files = ?, updated_at = ? WHERE token = ?',
    [seal(JSON.stringify(files)), Date.now(), token]
  )
  return result.affectedRows > 0
}
