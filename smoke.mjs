/**
 * smoke.mjs — prove the MCP server half works without a real harness.
 *
 * Runs `apply()` against a stub context, then speaks raw JSON-RPC to it over
 * HTTP exactly like `@deepseek-ai/dsh-mcp-client` would: initialize, the
 * initialized notification, tools/list, and a tool call. The tool call is
 * expected to *fail as a result* (there is no model behind this stub), which is
 * the behaviour that matters: a broken peer must come back as readable text,
 * not as a dead protocol connection.
 *
 *   node smoke.mjs
 */

import http from 'node:http'
import assert from 'node:assert/strict'
import { apply, parsePeerEnvelope, formatPeerEnvelope, splitBody, newMsgId } from './index.js'

const PORT = Number(process.env.SMOKE_PORT ?? 38499)
const BASE = `http://127.0.0.1:${PORT}`

const stubCtx = {
  services: new Map(),
  get(name) {
    return this.services.get(name)
  },
  on() {
    return () => {}
  },
  effect(callback) {
    callback?.()
    return () => {}
  },
  logger: () => {},
}

let nextId = 1

/** One POST to /mcp; returns { status, headers, json }. */
function rpc(body, sessionId) {
  return new Promise((resolve, reject) => {
    const payload = JSON.stringify(body)
    const headers = {
      'Content-Type': 'application/json',
      Accept: 'application/json, text/event-stream',
      'Content-Length': Buffer.byteLength(payload),
    }
    if (sessionId) headers['Mcp-Session-Id'] = sessionId
    const req = http.request(`${BASE}/mcp`, { method: 'POST', headers }, (res) => {
      const chunks = []
      res.on('data', (chunk) => chunks.push(chunk))
      res.on('end', () => {
        const text = Buffer.concat(chunks).toString('utf8')
        resolve({
          status: res.statusCode,
          headers: res.headers,
          json: text ? JSON.parse(text) : null,
        })
      })
    })
    req.on('error', reject)
    req.end(payload)
  })
}

function get(pathname) {
  return new Promise((resolve, reject) => {
    http
      .get(`${BASE}${pathname}`, (res) => {
        const chunks = []
        res.on('data', (chunk) => chunks.push(chunk))
        res.on('end', () => {
          resolve({ status: res.statusCode, text: Buffer.concat(chunks).toString('utf8') })
        })
      })
      .on('error', reject)
  })
}

const wait = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

const handle = apply(stubCtx, { port: PORT, hostLabel: 'smoke-host' })
await wait(300)

try {
  // 1. Health is plain HTTP so an operator can curl it without MCP.
  const health = await get('/health')
  assert.equal(health.status, 200)
  const info = JSON.parse(health.text)
  assert.equal(info.ok, true)
  assert.equal(info.service, 'dsh-peer')
  for (const tool of ['peer_ask', 'peer_sessions', 'peer_history', 'peer_status']) {
    assert.ok(info.tools.includes(tool), `health should list ${tool}`)
  }

  // 2. initialize negotiates a protocol and hands out a session id.
  const init = await rpc({
    jsonrpc: '2.0',
    id: nextId++,
    method: 'initialize',
    params: {
      protocolVersion: '2025-06-18',
      capabilities: {},
      clientInfo: { name: 'smoke', version: '0.0.0' },
    },
  })
  assert.equal(init.status, 200)
  assert.equal(init.json.result.protocolVersion, '2025-06-18')
  assert.equal(init.json.result.serverInfo.name, 'dsh-peer')
  assert.ok(init.json.result.capabilities.tools, 'must advertise the tools capability')
  const sessionId = init.headers['mcp-session-id']
  assert.ok(sessionId, 'initialize must return Mcp-Session-Id')

  // 3. Unknown protocol versions degrade to the newest supported one.
  const pinned = await rpc({
    jsonrpc: '2.0',
    id: nextId++,
    method: 'initialize',
    params: { protocolVersion: '1999-01-01', capabilities: {}, clientInfo: { name: 'smoke', version: '0' } },
  })
  assert.equal(pinned.json.result.protocolVersion, '2025-06-18')

  // 4. Notifications carry no id and get an empty 202.
  const notified = await rpc(
    { jsonrpc: '2.0', method: 'notifications/initialized' },
    sessionId,
  )
  assert.equal(notified.status, 202)
  assert.equal(notified.json, null)

  // 5. tools/list
  const list = await rpc({ jsonrpc: '2.0', id: nextId++, method: 'tools/list', params: {} }, sessionId)
  assert.equal(list.json.result.tools.length, 5)
  for (const tool of list.json.result.tools) {
    assert.equal(tool.inputSchema.type, 'object')
    assert.ok(tool.description.length > 20, `${tool.name} needs a real description`)
  }

  // 6. peer_status works with no model at all: it only reports facts.
  const status = await rpc(
    { jsonrpc: '2.0', id: nextId++, method: 'tools/call', params: { name: 'peer_status', arguments: {} } },
    sessionId,
  )
  assert.equal(status.json.result.isError, undefined)
  const statusText = status.json.result.content[0].text
  const parsed = JSON.parse(statusText)
  assert.equal(parsed.host, 'smoke-host')
  assert.ok(Array.isArray(parsed.sessions))

  // 7. A peer_ask with no model behind it must fail as a *result*, not a
  //    protocol error, so the calling agent can read the reason and adapt.
  const ask = await rpc(
    {
      jsonrpc: '2.0',
      id: nextId++,
      method: 'tools/call',
      params: { name: 'peer_ask', arguments: { prompt: 'ping', session: 'smoke' } },
    },
    sessionId,
  )
  assert.equal(ask.status, 200)
  assert.equal(ask.json.result.isError, true)
  assert.match(ask.json.result.content[0].text, /error:/)

  // 8. Sessions that were touched are listed afterwards.
  const sessions = await rpc(
    { jsonrpc: '2.0', id: nextId++, method: 'tools/call', params: { name: 'peer_sessions', arguments: {} } },
    sessionId,
  )
  const sessionText = sessions.json.result.content[0].text
  assert.match(sessionText, /peer:smoke/)

  // 9. Unknown methods are protocol errors.
  const bogus = await rpc({ jsonrpc: '2.0', id: nextId++, method: 'not/a/method' }, sessionId)
  assert.equal(bogus.json.error.code, -32601)

  // 10. GET on /mcp says "no server-initiated stream" instead of hanging.
  const stream = await get('/mcp')
  assert.equal(stream.status, 405)

  // 11. CHATv1 envelopes round-trip, and a broken header is reported instead of
  //     silently passing as a normal message.
  const msgId = newMsgId('pc')
  const header = formatPeerEnvelope({ from: 'pc', msg: msgId, reply: 'phone-20260101T000000Z-abc123' })
  const ok = parsePeerEnvelope(`${header}\nhello there`)
  assert.equal(ok.hasEnvelope, true)
  assert.deepEqual(ok.problems, [])
  assert.equal(ok.envelope.from, 'pc')
  assert.equal(ok.envelope.msg, msgId)
  assert.equal(ok.envelope.reply, 'phone-20260101T000000Z-abc123')
  assert.equal(ok.body, 'hello there')

  const bad = parsePeerEnvelope('[[CHATv1 from=PC! msg=short ts=yesterday]]\nbody')
  assert.equal(bad.hasEnvelope, true)
  assert.deepEqual(bad.problems.sort(), ['from', 'msg', 'ts'])
  assert.equal(bad.body, 'body')

  assert.equal(parsePeerEnvelope('just words').hasEnvelope, false)

  // 12. A long body splits into chunks that reassemble exactly.
  const long = 'x'.repeat(70000)
  const pieces = splitBody(long, 30000)
  assert.equal(pieces.length, 3)
  assert.equal(pieces.join(''), long)

  // 13. mode=async answers with a task id before the model would have replied,
  //     and the task is readable through peer_task afterwards.
  const started = await rpc(
    {
      jsonrpc: '2.0',
      id: nextId++,
      method: 'tools/call',
      params: { name: 'peer_ask', arguments: { prompt: 'slow one', session: 'smoke', mode: 'async' } },
    },
    sessionId,
  )
  const accepted = JSON.parse(started.json.result.content[0].text)
  assert.equal(accepted.status, 'accepted')
  assert.match(accepted.task_id, /^t-/)

  const polled = await rpc(
    {
      jsonrpc: '2.0',
      id: nextId++,
      method: 'tools/call',
      params: { name: 'peer_task', arguments: { taskId: accepted.task_id, waitMs: 2000 } },
    },
    sessionId,
  )
  const task = JSON.parse(polled.json.result.content[0].text)
  assert.equal(task.task_id, accepted.task_id)
  assert.ok(['done', 'failed'].includes(task.status), `task should settle, got ${task.status}`)

  // 14. Health reports the protocol state so an operator can see whether the
  //     peer is speaking CHATv1 at all.
  const health2 = JSON.parse((await get('/health')).text)
  assert.equal(health2.identity, null)
  assert.equal(health2.chatProtocol, false)
  assert.equal(health2.maxChunkChars, 60000)

  console.log('SMOKE_OK')
  console.log(`  endpoint : ${BASE}/mcp`)
  console.log(`  tools    : ${info.tools.join(', ')}`)
  console.log(`  sessions : ${info.sessions}`)
} finally {
  handle.close()
  await wait(100)
}
