/**
 * mcp-probe.mjs — speak MCP to a dsh-peer endpoint from the command line.
 *
 * Zero dependencies on purpose: it sends the same JSON-RPC 2.0 that
 * `@deepseek-ai/dsh-mcp-client` sends over its streamable-http transport, so a
 * pass here means the real client will connect too. Handy for checking a peer
 * before wiring it into a profile, and for calling arguments the client's cached
 * tool schema may not know yet (`mode`, `taskId`, `idempotencyKey`).
 *
 *   PEER_TOKEN=<token> node tools/mcp-probe.mjs http://192.0.2.20:38404/mcp ["prompt"]
 *
 * Env: PEER_TOKEN, PROBE_SLOT (default `probe`), PEER_ASK_TIMEOUT (ms).
 */

import http from 'node:http'
import https from 'node:https'
import { URL } from 'node:url'

const endpoint = process.argv[2] ?? 'http://127.0.0.1:38404/mcp'
const token = process.env.PEER_TOKEN ?? ''
const target = new URL(endpoint)
const lib = target.protocol === 'https:' ? https : http

let sessionId = null
let nextId = 1

function post(body) {
  return new Promise((resolve, reject) => {
    const payload = JSON.stringify(body)
    const headers = {
      'Content-Type': 'application/json',
      Accept: 'application/json, text/event-stream',
      'Content-Length': Buffer.byteLength(payload),
    }
    if (sessionId) headers['Mcp-Session-Id'] = sessionId
    if (token) headers.Authorization = `Bearer ${token}`
    const req = lib.request(
      { hostname: target.hostname, port: target.port, path: target.pathname, method: 'POST', headers },
      (res) => {
        const chunks = []
        res.on('data', (chunk) => chunks.push(chunk))
        res.on('end', () => {
          const text = Buffer.concat(chunks).toString('utf8')
          const sid = res.headers['mcp-session-id']
          if (sid) sessionId = sid
          resolve({ status: res.statusCode, text })
        })
      },
    )
    req.on('error', reject)
    req.end(payload)
  })
}

async function call(method, params) {
  const reply = await post({ jsonrpc: '2.0', id: nextId++, method, params })
  if (reply.status === 401) throw new Error('unauthorized: set PEER_TOKEN')
  if (reply.status !== 200) throw new Error(`${method}: HTTP ${reply.status} ${reply.text.slice(0, 200)}`)
  const body = JSON.parse(reply.text)
  if (body.error) throw new Error(`${method}: ${body.error.message}`)
  return body.result
}

async function notify(method) {
  const reply = await post({ jsonrpc: '2.0', method })
  if (reply.status !== 202) throw new Error(`${method}: expected 202, got ${reply.status}`)
}

const started = Date.now()
const init = await call('initialize', {
  protocolVersion: '2025-06-18',
  capabilities: {},
  clientInfo: { name: 'mcp-probe', version: '0.0.0' },
})
console.log(`initialize  : protocol=${init.protocolVersion} server=${init.serverInfo.name} session=${sessionId}`)
await notify('notifications/initialized')

const list = await call('tools/list', {})
console.log(`tools/list  : ${list.tools.map((tool) => tool.name).join(', ')}`)

const status = await call('tools/call', { name: 'peer_status', arguments: {} })
const statusText = status.content?.[0]?.text ?? ''
let parsed = null
try {
  parsed = JSON.parse(statusText)
} catch {
  /* not JSON, print raw below */
}
if (parsed) {
  console.log(`peer_status : host=${parsed.host} platform=${parsed.platform} sessions=${parsed.sessions.length}`)
  console.log(`              model=${JSON.stringify(parsed.model)}`)
  if (parsed.identity) console.log(`              identity=${parsed.identity} sandbox=${parsed.sandboxMode ?? '-'}`)
} else {
  console.log(`peer_status : ${statusText.slice(0, 200)}`)
}

const SLOT = process.env.PROBE_SLOT ?? 'probe'
const history = await call('tools/call', { name: 'peer_history', arguments: { session: SLOT, limit: 4 } })
console.log(`peer_history: ${(history.content?.[0]?.text ?? '').replace(/\s+/g, ' ').slice(0, 240)}`)

const prompt = process.argv[3] ?? 'In one sentence, say which machine you are running on.'
const askArgs = { prompt, session: SLOT }
if (process.env.PEER_ASK_TIMEOUT) askArgs.timeoutMs = Number(process.env.PEER_ASK_TIMEOUT)
const ask = await call('tools/call', { name: 'peer_ask', arguments: askArgs })
const askText = ask.content?.[0]?.text ?? ''
const elapsed = Date.now() - started
if (ask.isError) {
  console.log(`peer_ask    : FAILED (${elapsed}ms): ${askText.slice(0, 400)}`)
  process.exitCode = 1
} else {
  console.log(`peer_ask    : ${elapsed}ms, ${askText.length} chars`)
  console.log(`              ${askText.replace(/\n+/g, ' ').slice(0, 300)}`)
}

const after = await call('tools/call', { name: 'peer_sessions', arguments: {} })
console.log(`sessions    : ${(after.content?.[0]?.text ?? '').replace(/\s+/g, ' ').slice(0, 300)}`)
