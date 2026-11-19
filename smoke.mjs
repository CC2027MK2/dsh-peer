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
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import crypto from 'node:crypto'
import assert from 'node:assert/strict'
import { apply, parsePeerEnvelope, formatPeerEnvelope, splitBody, newMsgId } from './index.js'

const PORT = Number(process.env.SMOKE_PORT ?? 38499)
const BASE = `http://127.0.0.1:${PORT}`
// Keep every run isolated: rooms and tasks persist, and a leftover event would
// make the seq assertions order-dependent.
const DATA_DIR = path.join(os.tmpdir(), `dsh-peer-smoke-${process.pid}-${Date.now()}`)
process.env.DSH_PEER_DATA = DATA_DIR

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
function rpc(body, sessionId, extraHeaders = {}) {
  return new Promise((resolve, reject) => {
    const payload = JSON.stringify(body)
    const headers = {
      'Content-Type': 'application/json',
      Accept: 'application/json, text/event-stream',
      'Content-Length': Buffer.byteLength(payload),
      ...extraHeaders,
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

const handle = apply(stubCtx, {
  port: PORT,
  hostLabel: 'smoke-host',
  identity: 'smoke',
  sandboxMode: 'danger-full-access',
  strictArtifacts: true,
  artifactHosts: ['127.0.0.1'],
  capabilities: { tools: ['bash', 'read'], shell: 'pwsh', writableRoots: ['/tmp'] },
  rooms: { lobby: { members: ['smoke', 'phone'] } },
})
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
  assert.equal(list.json.result.tools.length, 12)
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
  assert.equal(health2.identity, 'smoke')
  assert.equal(health2.chatProtocol, true)
  assert.equal(health2.maxChunkChars, 60000)

  // 15. Capabilities are declared, not guessed: the caller can see the shell,
  //     the sandbox and the artifact budget before sending work.
  const caps = health2.capabilities
  assert.equal(caps.protocol, 'chatv1.1')
  assert.equal(caps.identity, 'smoke')
  assert.equal(caps.sandbox, 'danger-full-access')
  assert.equal(caps.shell, 'pwsh')
  assert.deepEqual(caps.tools, ['bash', 'read'])
  assert.equal(caps.artifacts.maxBytes, 64 * 1024 * 1024)
  assert.equal(caps.limits.maxSlots, 8)
  const statusCaps = JSON.parse(
    (
      await rpc(
        { jsonrpc: '2.0', id: nextId++, method: 'tools/call', params: { name: 'peer_status', arguments: {} } },
        sessionId,
      )
    ).json.result.content[0].text,
  ).capabilities
  assert.equal(statusCaps.protocol, 'chatv1.1')

  // 16. require is checked before the agent runs: a sandbox mismatch answers
  //     instantly instead of burning a turn.
  const refused = await rpc(
    {
      jsonrpc: '2.0',
      id: nextId++,
      method: 'tools/call',
      params: {
        name: 'peer_ask',
        arguments: { prompt: 'do it', session: 'smoke', require: { sandbox: 'workspace-write' } },
      },
    },
    sessionId,
  )
  const refusedPayload = JSON.parse(refused.json.result.content[0].text)
  assert.equal(refusedPayload.status, 'failed')
  assert.match(refusedPayload.error, /requirements not met/)
  assert.ok(
    refusedPayload.output.missing.some((entry) => entry.startsWith('sandbox:workspace-write')),
    'must report the sandbox mismatch',
  )

  // 17. A declared tool the host does not have fails preflight too, while an
  //     undeclared tool list reports "unverified" instead of blocking.
  const missingTool = JSON.parse(
    (
      await rpc(
        {
          jsonrpc: '2.0',
          id: nextId++,
          method: 'tools/call',
          params: { name: 'peer_ask', arguments: { prompt: 'x', session: 'smoke', require: { tools: ['browser'] } } },
        },
        sessionId,
      )
    ).json.result.content[0].text,
  )
  assert.equal(missingTool.status, 'failed')
  assert.ok(missingTool.output.missing.includes('tool:browser'))

  // 18. Artifacts are hashed before delivery: a good file is accepted, a bad
  //     digest is refused with the reason, and an undeclared URL in the body is
  //     rejected once strict mode is on.
  const tmpFile = path.join(os.tmpdir(), `dsh-peer-smoke-${Date.now()}.txt`)
  fs.writeFileSync(tmpFile, 'handoff payload')
  const digest = crypto.createHash('sha256').update('handoff payload').digest('hex')
  const goodArtifact = await rpc(
    {
      jsonrpc: '2.0',
      id: nextId++,
      method: 'tools/call',
      params: {
        name: 'peer_ask',
        arguments: {
          prompt: 'consume this',
          session: 'smoke',
          mode: 'async',
          artifacts: [{ role: 'input', path: tmpFile, sha256: digest, size: 15 }],
        },
      },
    },
    sessionId,
  )
  const acceptedTask = JSON.parse(goodArtifact.json.result.content[0].text)
  assert.equal(acceptedTask.status, 'accepted', 'artifact check must pass before the task starts')

  const badArtifact = JSON.parse(
    (
      await rpc(
        {
          jsonrpc: '2.0',
          id: nextId++,
          method: 'tools/call',
          params: {
            name: 'peer_ask',
            arguments: {
              prompt: 'consume this',
              session: 'smoke',
              artifacts: [{ role: 'input', path: tmpFile, sha256: 'deadbeef' }],
            },
          },
        },
        sessionId,
      )
    ).json.result.content[0].text,
  )
  assert.equal(badArtifact.status, 'failed')
  assert.match(badArtifact.error, /artifact verification failed/)
  assert.match(badArtifact.output.problems[0].error, /sha256 mismatch/)

  const undeclared = JSON.parse(
    (
      await rpc(
        {
          jsonrpc: '2.0',
          id: nextId++,
          method: 'tools/call',
          params: {
            name: 'peer_ask',
            arguments: { prompt: 'fetch http://127.0.0.1:9/nothing.bin and use it', session: 'smoke' },
          },
        },
        sessionId,
      )
    ).json.result.content[0].text,
  )
  assert.equal(undeclared.status, 'failed')
  assert.match(undeclared.error, /artifact references are not declared/)
  assert.deepEqual(undeclared.output.missing_artifacts, ['http://127.0.0.1:9/nothing.bin'])
  fs.unlinkSync(tmpFile)

  // 19. Rooms: a hub keeps one ordered log, members read it with a seq cursor,
  //     and addressing checks membership instead of silently dropping the line.
  const say = await rpc(
    {
      jsonrpc: '2.0',
      id: nextId++,
      method: 'tools/call',
      params: { name: 'room_say', arguments: { room: 'lobby', text: 'first line' } },
    },
    sessionId,
  )
  const said = JSON.parse(say.json.result.content[0].text)
  assert.equal(said.room, 'lobby')
  assert.equal(said.seq, 1)
  assert.equal(said.from, 'smoke')
  assert.equal(said.to, null)

  const sync = await rpc(
    {
      jsonrpc: '2.0',
      id: nextId++,
      method: 'tools/call',
      params: { name: 'room_sync', arguments: { room: 'lobby', since: 0 } },
    },
    sessionId,
  )
  const synced = JSON.parse(sync.json.result.content[0].text)
  assert.equal(synced.count, 1)
  assert.equal(synced.events[0].text, 'first line')
  assert.equal(synced.seq, 1)

  // A cursor that is already up to date returns nothing, so a member can poll
  // without replaying the room each time.
  const idle = JSON.parse(
    (
      await rpc(
        { jsonrpc: '2.0', id: nextId++, method: 'tools/call', params: { name: 'room_sync', arguments: { room: 'lobby', since: 1 } } },
        sessionId,
      )
    ).json.result.content[0].text,
  )
  assert.equal(idle.count, 0)

  // 20. Addressing a non-member is an error, not a whisper nobody receives.
  const badTo = await rpc(
    {
      jsonrpc: '2.0',
      id: nextId++,
      method: 'tools/call',
      params: { name: 'room_say', arguments: { room: 'lobby', text: 'hi', to: 'nobody' } },
    },
    sessionId,
  )
  assert.equal(badTo.json.result.isError, true)
  assert.match(badTo.json.result.content[0].text, /not a member/)

  // 21. An unconfigured room is refused; rooms are declared, not created by
  //     whoever asks for one.
  const badRoom = await rpc(
    {
      jsonrpc: '2.0',
      id: nextId++,
      method: 'tools/call',
      params: { name: 'room_say', arguments: { room: 'nowhere', text: 'hi' } },
    },
    sessionId,
  )
  assert.equal(badRoom.json.result.isError, true)
  assert.match(badRoom.json.result.content[0].text, /unknown room/)

  // 22. room_list summarises every configured room.
  const rooms = JSON.parse(
    (
      await rpc(
        { jsonrpc: '2.0', id: nextId++, method: 'tools/call', params: { name: 'room_list', arguments: {} } },
        sessionId,
      )
    ).json.result.content[0].text,
  )
  assert.equal(rooms.rooms.length, 1)
  assert.equal(rooms.rooms[0].room, 'lobby')
  assert.deepEqual(rooms.rooms[0].members, ['smoke', 'phone'])
  assert.equal(rooms.rooms[0].seq, 1)

  // 23. A member's client row can name itself, so the hub attributes the line
  //     to the sender instead of to the hub operator.
  const asPhone = await rpc(
    {
      jsonrpc: '2.0',
      id: nextId++,
      method: 'tools/call',
      params: { name: 'room_say', arguments: { room: 'lobby', text: 'hello from the phone' } },
    },
    sessionId,
    { 'X-Peer-Identity': 'phone' },
  )
  const phoneLine = JSON.parse(asPhone.json.result.content[0].text)
  assert.equal(phoneLine.from, 'phone')
  assert.equal(phoneLine.seq, 2)

  // 24. Redelivery view: an unread line stays in the member's pending list
  //     until it is acknowledged, no matter how often the member reconnects.
  const pending = await rpc(
    {
      jsonrpc: '2.0',
      id: nextId++,
      method: 'tools/call',
      params: { name: 'room_pending', arguments: { room: 'lobby' } },
    },
    sessionId,
    { 'X-Peer-Identity': 'phone' },
  )
  const pend = JSON.parse(pending.json.result.content[0].text)
  assert.equal(pend.member, 'phone')
  assert.ok(pend.count >= 1, 'the unread line must be redelivered')
  assert.ok(
    pend.pending.every((item) => item.seq !== 2),
    'a member does not owe a receipt for its own line',
  )

  // 25. room_ack clears it.
  const acked = await rpc(
    {
      jsonrpc: '2.0',
      id: nextId++,
      method: 'tools/call',
      params: { name: 'room_ack', arguments: { room: 'lobby', seq: 2 } },
    },
    sessionId,
    { 'X-Peer-Identity': 'phone' },
  )
  const ack = JSON.parse(acked.json.result.content[0].text)
  assert.ok(ack.updated >= 1)
  const afterAck = JSON.parse(
    (
      await rpc(
        { jsonrpc: '2.0', id: nextId++, method: 'tools/call', params: { name: 'room_pending', arguments: { room: 'lobby' } } },
        sessionId,
        { 'X-Peer-Identity': 'phone' },
      )
    ).json.result.content[0].text,
  )
  assert.equal(afterAck.count, 0)

  // 26. Ownership: the first claim wins and the second is refused with the
  //     owner's name, so two members cannot quietly fork the same work.
  const claimed = await rpc(
    {
      jsonrpc: '2.0',
      id: nextId++,
      method: 'tools/call',
      params: { name: 'room_claim', arguments: { room: 'lobby', seq: 1 } },
    },
    sessionId,
    { 'X-Peer-Identity': 'phone' },
  )
  assert.equal(JSON.parse(claimed.json.result.content[0].text).owner, 'phone')
  const contest = await rpc(
    {
      jsonrpc: '2.0',
      id: nextId++,
      method: 'tools/call',
      params: { name: 'room_claim', arguments: { room: 'lobby', seq: 1 } },
    },
    sessionId,
    { 'X-Peer-Identity': 'smoke' },
  )
  assert.equal(contest.json.result.isError, true)
  assert.match(contest.json.result.content[0].text, /already owned by phone/)

  // 27. Acceptance: only the owner signs off, and the verdict is recorded.
  const accepted2 = await rpc(
    {
      jsonrpc: '2.0',
      id: nextId++,
      method: 'tools/call',
      params: { name: 'room_accept', arguments: { room: 'lobby', seq: 1, verdict: 'accepted', note: 'read and done' } },
    },
    sessionId,
    { 'X-Peer-Identity': 'phone' },
  )
  const verdict = JSON.parse(accepted2.json.result.content[0].text)
  assert.equal(verdict.verdict, 'accepted')
  assert.equal(verdict.by, 'phone')
  assert.equal(verdict.note, 'read and done')
  const outsider = await rpc(
    {
      jsonrpc: '2.0',
      id: nextId++,
      method: 'tools/call',
      params: { name: 'room_accept', arguments: { room: 'lobby', seq: 1, verdict: 'rejected' } },
    },
    sessionId,
    { 'X-Peer-Identity': 'smoke' },
  )
  assert.equal(outsider.json.result.isError, true)
  assert.match(outsider.json.result.content[0].text, /only the owner can accept or reject/)

  console.log('SMOKE_OK')
  console.log(`  endpoint : ${BASE}/mcp`)
  console.log(`  tools    : ${info.tools.join(', ')}`)
  console.log(`  sessions : ${info.sessions}`)
} finally {
  handle.close()
  await wait(100)
  try {
    fs.rmSync(DATA_DIR, { recursive: true, force: true })
  } catch {
    /* best effort */
  }
}
