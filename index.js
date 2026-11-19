/**
 * dsh-peer / index.js
 *
 * Lets the DSH running on *another* machine drive the agents of this one.
 *
 * DSH ships an MCP client but no MCP server, so the missing half is here: a
 * dependency-free MCP server over the "streamable HTTP" transport. Point the
 * other host's `@deepseek-ai/dsh-mcp-client` at
 *
 *     { transport: 'streamable-http', url: 'http://<this-host>:38404/mcp' }
 *
 * and every agent session on this host becomes a tool over there
 * (`mcp__<serverName>__peer_ask`, …). Nothing about the peer is special-cased:
 * this is the same JSON-RPC surface any MCP server exposes, which is why it
 * works in both directions — install this bundle on each machine and either
 * side can call the other.
 *
 * Tools
 *   peer_ask       run one turn on a named conversation slot, return the reply
 *   peer_sessions  list the slots this bridge knows about
 *   peer_history   replay the last messages of one slot
 *   peer_status    host, model selection, and whether agents are available
 *
 * Why the tools map to agents rather than to a shell: a peer call must mean
 * "ask the other agent", not "run a command over there". The agent keeps its
 * own tools, its own memory, and its own permission model.
 */

import http from 'node:http'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import crypto from 'node:crypto'

const SERVER_NAME = 'dsh-peer'
// Read the version from package.json so a deploy cannot report a stale number.
const VERSION = (() => {
  try {
    const here = path.dirname(new URL(import.meta.url).pathname.replace(/^\/([A-Za-z]:)/, '$1'))
    return JSON.parse(fs.readFileSync(path.join(here, 'package.json'), 'utf8')).version ?? '0.0.0'
  } catch {
    return '0.0.0'
  }
})()
const DEFAULT_PORT = 38404
const stateKeyFor = (port) => Symbol.for(`dsh-peer.server:${port}`)

// Protocol revisions this server speaks. A client that asks for something newer
// gets its own version echoed back only when it is on this list; otherwise the
// latest supported one is announced, which is what the spec expects.
const SUPPORTED_PROTOCOLS = ['2025-06-18', '2025-03-26', '2024-11-05']
const LATEST_PROTOCOL = SUPPORTED_PROTOCOLS[0]

const MAX_BODY_BYTES = 1 << 20
const MAX_HISTORY = 40
const DEFAULT_ASK_TIMEOUT_MS = 240000
/** How long a degraded slot waits before retrying the agent handshake. */
const AGENT_RETRY_MS = 30_000
/** Upper bound on live conversation slots; the least recently used is dropped. */
const DEFAULT_MAX_SLOTS = 8
const DEFAULT_MAX_CHUNK_CHARS = 60000
const DEFAULT_TASK_TIMEOUT_MS = 1800000

/* ------------------------------------------------------------------------- *
 * CHATv1 envelope
 *
 * The peer vocabulary (`[[CHATv1 from=… msg=… ts=… chain=… reply=…]]` + body)
 * used to live only in a preset prompt, which meant a model that drifted took
 * the protocol with it. These helpers make the shape a fact of the plugin: a
 * message either parses or it does not, ids can be deduplicated, and long
 * bodies can be split into `chunk=i/n` parts on the way out.
 * ------------------------------------------------------------------------- */

const CHATV1_HEADER = /^\[\[CHATv1\s+([^\]\n]*)\]\]\s*\n?/
const ID_RE = /^[A-Za-z0-9][A-Za-z0-9-]{5,63}$/
const TS_RE = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,3})?Z$/
const CHUNK_RE = /^(\d{1,4})\/(\d{1,4})$/

/** Stable, sortable message id: `<from>-<utc>-<base36>`. */
function newMsgId(from, stamp = Date.now()) {
  const who = String(from || 'peer').toLowerCase().replace(/[^a-z0-9]/g, '').slice(0, 8) || 'peer'
  const utc = `${new Date(stamp).toISOString().slice(0, 19).replace(/[-:]/g, '')}Z`
  const rand = Math.random().toString(36).slice(2, 8).padEnd(6, '0').toLowerCase()
  // Only the name and the random tail are lower-cased: the UTC stamp keeps its
  // canonical T/Z so the id stays readable next to a `ts=` field.
  return `${who}-${utc}-${rand}`
}

/**
 * Parse a peer message. Never throws: an unparseable header is reported as
 * `malformed` so the caller can fall back to treating the text as a user line.
 */
function parsePeerEnvelope(text) {
  const raw = typeof text === 'string' ? text : ''
  const match = CHATV1_HEADER.exec(raw)
  if (!match) return { hasEnvelope: false, envelope: null, body: raw, problems: [] }
  const fields = {}
  for (const token of match[1].split(/\s+/)) {
    const eq = token.indexOf('=')
    if (eq <= 0) continue
    const key = token.slice(0, eq).toLowerCase()
    const value = token.slice(eq + 1)
    if (value) fields[key] = value
  }
  const problems = []
  if (!/^[a-z][a-z0-9_-]{0,31}$/.test(fields.from ?? '')) problems.push('from')
  if (!ID_RE.test(fields.msg ?? '')) problems.push('msg')
  if (!TS_RE.test(fields.ts ?? '')) problems.push('ts')
  if (fields.chain !== undefined && fields.chain !== '-' && !ID_RE.test(fields.chain)) problems.push('chain')
  if (fields.reply !== undefined && fields.reply !== '-' && !ID_RE.test(fields.reply)) problems.push('reply')
  if (fields.chunk !== undefined && !CHUNK_RE.test(fields.chunk)) problems.push('chunk')
  const body = raw.slice(match[0].length)
  if (problems.length > 0) {
    return { hasEnvelope: true, envelope: { ...fields, raw: match[0].trim() }, body, problems }
  }
  return {
    hasEnvelope: true,
    envelope: {
      from: fields.from,
      msg: fields.msg,
      ts: fields.ts,
      chain: fields.chain ?? '-',
      reply: fields.reply ?? '-',
      chunk: fields.chunk ?? '',
    },
    body,
    problems: [],
  }
}

/** Render one envelope line (no body). */
function formatPeerEnvelope({ from, msg, ts, chain = '-', reply = '-', chunk = '' }) {
  const stamp = ts ?? new Date().toISOString().replace(/\.\d{3}Z$/, 'Z')
  return `[[CHATv1 from=${from} msg=${msg} ts=${stamp} chain=${chain} reply=${reply}${chunk ? ` chunk=${chunk}` : ''}]]`
}

/** Split a body into `max`-sized pieces without cutting surrogate pairs. */
function splitBody(text, max) {
  const limit = Math.max(1000, Number(max) || 60000)
  if (text.length <= limit) return [text]
  const parts = []
  let index = 0
  while (index < text.length) {
    let end = Math.min(text.length, index + limit)
    const code = text.charCodeAt(end - 1)
    if (end < text.length && code >= 0xd800 && code <= 0xdbff) end -= 1
    parts.push(text.slice(index, end))
    index = end
  }
  return parts
}

/** Pull the reader-facing text out of one `assistant/message` session event.
 *
 * An assistant message carries every part the model produced in that step:
 * `reasoning` (its private thinking), `tool-call`, and finally `text`. Only the
 * last kind may leave the building — concatenating every part that happens to
 * have a `.text` field leaks the chain of thought to the peer.
 */
function extractAssistantText(event) {
  const candidates = [event?.data?.message, event?.message, event?.payload?.message]
  for (const message of candidates) {
    if (!message || typeof message !== 'object') continue
    const content = message.content
    if (!Array.isArray(content)) continue
    const parts = []
    for (const part of content) {
      if (part && part.type === 'text' && typeof part.text === 'string' && part.text.trim()) {
        parts.push(part.text)
      }
    }
    if (parts.length > 0) return parts.join('\n').trim()
  }
  return ''
}

/** Last assistant text at or after `from` in a snapshot of session events. */
function lastAssistantText(events, from) {
  for (let index = events.length - 1; index >= Math.max(from, 0); index -= 1) {
    const event = events[index]
    if (event?.type !== 'assistant/message') continue
    const text = extractAssistantText(event)
    if (text) return text
  }
  return ''
}

/** First approval request the far-side agent is blocked on, if any. */
function findApproval(events, from) {
  let asked = null
  let askedIndex = -1
  let decidedIndex = -1
  for (let index = Math.max(from, 0); index < events.length; index += 1) {
    const event = events[index]
    if (event?.type === 'approval/asked') {
      askedIndex = index
      const data = event?.data ?? {}
      asked = {
        type: event.type,
        toolName: String(data.toolName ?? data.name ?? 'unknown'),
        reason: String(data.reason ?? ''),
      }
    } else if (event?.type === 'approval/decided') {
      decidedIndex = index
    }
  }
  // A decision that follows the request means the turn is not actually blocked
  // (auto-approve, or a human who clicked fast) — never report that as need_user.
  if (!asked || decidedIndex > askedIndex) return null
  return asked
}

/** One conversation slot: a peer-named session plus its live agent handle. */
class Slot {
  constructor({ host, sessionId }) {
    this.host = host
    this.sessionId = sessionId
    this.agent = null
    this.handle = null
    this.createUserMessage = null
    this.unavailable = null
    this.unavailableAt = 0
    this.sandboxPinned = false
    this.turns = 0
    this.lastUsed = Date.now()
    this.queue = Promise.resolve()
  }

  /**
   * Pin this session's sandbox mode, once per slot.
   *
   * A host whose sandbox backend cannot enforce `workspace-write` (a PRoot or
   * otherwise restricted container) fails every confined command closed, so a
   * peer session that must run commands needs the session-level override that
   * `dsh-sandbox-policy` folds from a `sandbox/mode` event. Nothing mutates the
   * mode out of band: the event IS the switch, so it survives a restart.
   */
  #pinSandboxMode() {
    const mode = this.host.sandboxMode
    if (!mode || this.sandboxPinned) return
    const session = this.agent?.session ?? this.handle?.session
    if (typeof session?.append !== 'function') return
    try {
      const events = typeof session.snapshotEvents === 'function' ? session.snapshotEvents() : []
      const last = [...events].reverse().find((event) => event?.type === 'sandbox/mode')
      if (last?.data?.mode !== mode) session.append('sandbox/mode', { mode })
      this.sandboxPinned = true
      this.host.log(`agent: sandbox mode pinned to ${mode} session=${this.sessionId}`)
    } catch (err) {
      this.host.log(`agent: sandbox mode pin failed: ${String(err?.message ?? err)}`)
    }
  }

  /** Serialize turns: one agent handle cannot run two turns at once. */
  ask(prompt, timeoutMs) {
    const run = this.queue.then(() => this.#run(prompt, timeoutMs))
    // Keep the chain alive even when a turn rejects, otherwise one failure
    // would poison every later call on this slot.
    this.queue = run.then(
      () => undefined,
      () => undefined,
    )
    return run
  }

  async #run(prompt, timeoutMs) {
    const agent = await this.#agent()
    this.turns += 1
    this.lastUsed = Date.now()
    if (agent === null) return this.#runDirect(prompt)
    return this.#runSession(agent, prompt, timeoutMs)
  }

  /**
   * Attach to a real DSH session inside this host.
   *
   * `@deepseek-ai/*` only resolves when the plugin directory can see the host's
   * node_modules (a `link:` mount does not, hence the symlink the install notes
   * describe). When it cannot be imported we fall back to a plain model call
   * instead of failing the tool.
   */
  async #agent() {
    if (this.agent !== null) return this.agent
    if (this.unavailable !== null) {
      // A broken import or a preset that was not mounted yet used to disable the
      // slot for the rest of the process. The usual causes clear on their own,
      // so retry the handshake after a grace period instead of staying degraded.
      if (Date.now() - this.unavailableAt < AGENT_RETRY_MS) return null
      this.host.log(`agent: retrying after "${this.unavailable}"`)
      this.unavailable = null
    }
    try {
      const agentModule = await import('@deepseek-ai/dsh-agent')
      const llmModule = await import('@deepseek-ai/dsh-llm')
      const installModelSelection = agentModule.installModelSelection
      const createUserMessage = llmModule.createUserMessage
      if (typeof installModelSelection !== 'function') throw new Error('installModelSelection missing')
      if (typeof createUserMessage !== 'function') throw new Error('createUserMessage missing')
      this.createUserMessage = createUserMessage

      const agents = this.host.ctx.get('agents')
      if (!agents) throw new Error('agents service unavailable')

      const existing = typeof agents.get === 'function' ? agents.get(this.sessionId) : undefined
      if (existing) {
        this.agent = existing
        this.handle = null
        this.host.log(`agent: reusing live session ${this.sessionId}`)
        return this.agent
      }

      const defaults = this.host.ctx.get('agentDefaultModel')
      const presets = this.host.ctx.get('agentPresets')

      // A peer agent must never reach back into the other device: the profile
      // mounts the reverse-bridge MCP client globally, so mask those tool names
      // inside this agent's own scope. Unknown names are skipped (the MCP
      // server may be offline); a failure here is logged, never fatal.
      const restrictPeerTools = (agentCtx) => {
        const deny = this.host.denyTools
        if (!Array.isArray(deny) || deny.length === 0) return
        try {
          const tools = typeof agentCtx.get === 'function' ? agentCtx.get('tools') : undefined
          if (!tools || typeof tools.restrict !== 'function') return
          let known
          try {
            known = tools.view(agentCtx)?.restrictableNames
          } catch {
            known = undefined
          }
          const names = known instanceof Set ? deny.filter((name) => known.has(name)) : deny
          if (names.length === 0) return
          tools.restrict({ deny: names })
          this.host.log(`agent: peer tools denied: ${names.join(',')}`)
        } catch (err) {
          this.host.log(`agent: peer tool restriction failed: ${err?.message ?? err}`)
        }
      }

      let composition = {
        setup: (agentCtx) => {
          installModelSelection(agentCtx)
          restrictPeerTools(agentCtx)
        },
      }
      const wanted = this.host.agentPreset
      if (presets && typeof presets.resolve === 'function' && typeof presets.mount === 'function') {
        try {
          const resolved = await presets.resolve(wanted || undefined)
          if (resolved?.id) {
            composition = {
              agentPreset: resolved.id,
              setup: async (agentCtx) => {
                installModelSelection(agentCtx)
                await presets.mount(agentCtx, resolved.id)
                restrictPeerTools(agentCtx)
              },
            }
          }
        } catch (err) {
          this.host.log(`agent: preset unavailable, using bare setup: ${err?.message ?? err}`)
        }
      }

      const selection = () => {
        try {
          return defaults?.currentSelection?.() ?? {}
        } catch {
          return {}
        }
      }
      const createOptions = {
        sessionId: this.sessionId,
        agentOptions: {
          get provider() {
            return selection().provider
          },
          get model() {
            return selection().model
          },
          get reasoningEffort() {
            return selection().reasoningEffort
          },
        },
        meta: {
          cwd: this.host.cwd,
          ...(composition.agentPreset ? { agentPreset: composition.agentPreset } : {}),
        },
        setup: composition.setup,
      }

      let handle
      try {
        handle = await agents.create(createOptions)
      } catch (err) {
        const reason = String(err?.message ?? err)
        // A stale session file is the only case resume can repair. Anything
        // else (bad plugin config, missing preset) must surface as-is: resuming
        // into a half-created session yields turns that end with no reply.
        if (!/already exists/i.test(reason)) throw new Error(`agent create failed: ${reason}`)
        this.host.log(`agent: session exists, resuming ${this.sessionId}`)
        handle = await agents.resume({ resumeSessionId: this.sessionId })
      }
      this.handle = handle ?? null
      this.agent = handle?.agent ?? handle
      this.#pinSandboxMode()
      this.host.log(`agent: ready session=${this.sessionId} preset=${composition.agentPreset ?? '(none)'}`)
      return this.agent
    } catch (err) {
      this.unavailable = String(err?.message ?? err)
      this.unavailableAt = Date.now()
      this.host.log(`agent: unavailable, falling back to a plain model call: ${this.unavailable}`)
      return null
    }
  }

  /**
   * Drop the live handle.
   *
   * Used when the slot is evicted or the plugin unloads: without this the agent
   * keeps its session lock (and its whole event log) alive in the host.
   */
  async dispose() {
    const handle = this.handle
    this.handle = null
    this.agent = null
    this.unavailable = null
    this.unavailableAt = 0
    if (!handle) return
    try {
      if (typeof handle.dispose === 'function') await handle.dispose()
      else if (typeof handle.agent?.dispose === 'function') await handle.agent.dispose()
      this.host.log(`slot: disposed ${this.sessionId}`)
    } catch (err) {
      this.host.log(`slot: dispose failed for ${this.sessionId}: ${err?.message ?? err}`)
    }
  }

  async #runSession(agent, prompt, timeoutMs) {
    const session = agent.session
    const before = typeof session?.snapshotEvents === 'function' ? session.snapshotEvents().length : 0
    const started = Date.now()
    let failure = null
    const stopErrors =
      typeof this.host.ctx.on === 'function'
        ? this.host.ctx.on('agent/error', ({ agent: subject, error }) => {
            if (subject === agent) failure = error
          })
        : () => {}
    try {
      await agent.followup(
        this.createUserMessage({ content: [{ type: 'text', text: prompt }], source: { kind: 'user' } }),
      )
      const deadline = Date.now() + Math.max(1000, timeoutMs)
      while (Date.now() < deadline) {
        const events = typeof session?.snapshotEvents === 'function' ? session.snapshotEvents() : []
        // The far-side agent may be parked on a tool-approval prompt that only a
        // human can answer. Hand that back as `need_user` at once: holding the
        // request until the deadline ends in a timeout nobody can act on.
        const approval = findApproval(events, Math.max(before, 0))
        if (approval) {
          this.host.log(`ask: approval needed (${approval.toolName}) in ${this.sessionId}`)
          return JSON.stringify(
            {
              v: 1,
              status: 'need_user',
              duration_ms: Date.now() - started,
              output: { approval },
              evidence: [],
              error: null,
              no_forward_ack: true,
            },
            null,
            2,
          )
        }
        let ended = false
        for (let index = Math.max(before, 0); index < events.length; index += 1) {
          if (events[index]?.type === 'turn/end') ended = true
        }
        // Only answer once the turn is closed: one turn may contain several
        // steps, and only the last assistant message is the actual reply.
        if (ended) {
          const reply = lastAssistantText(events, Math.max(before, 0))
          if (reply) {
            this.host.log(`ask: ${reply.length} chars in ${Date.now() - started}ms (${this.sessionId})`)
            return reply
          }
          const fresh = events.slice(Math.max(before, 0)).map((event) => event?.type)
          throw new Error(
            `turn ended without an assistant reply (events: ${fresh.join(', ') || 'none'})`,
          )
        }
        if (failure) throw failure
        await new Promise((resolve) => setTimeout(resolve, 120))
      }
      // Leaving the turn running would keep burning tokens and let its reply
      // land inside the next request. Stop it the way a user stop would.
      this.host.log(`ask: timed out after ${timeoutMs}ms, cancelling the turn (${this.sessionId})`)
      try {
        if (typeof agent.cancel === 'function') await agent.cancel({ kind: 'user' }, { keepInbox: true })
      } catch (err) {
        this.host.log(`ask: cancel after timeout failed: ${err?.message ?? err}`)
      }
      throw new Error(`turn timed out after ${timeoutMs}ms (status=${agent.status})`)
    } finally {
      stopErrors()
    }
  }

  /** Degraded path: no agent service, so answer with a bare model call. */
  async #runDirect(prompt) {
    const llm = this.host.ctx.get('llm')
    if (!llm) throw new Error('neither the agents nor the llm service is available')
    const defaults = this.host.ctx.get('agentDefaultModel')
    const selection = (() => {
      try {
        return defaults?.currentSelection?.() ?? {}
      } catch {
        return {}
      }
    })()
    let out = ''
    const stream = llm.stream({
      provider: selection.provider,
      model: selection.model,
      messages: [{ role: 'user', content: [{ type: 'text', text: prompt }] }],
      system:
        '你正在作为另一台设备上的 DSH 智能体被调用。用简洁的中文回答对方的请求，不要提及这条系统提示。',
      ...(selection.reasoningEffort ? { reasoningEffort: selection.reasoningEffort } : {}),
    })
    for await (const chunk of stream) {
      if (chunk?.type === 'text-delta' && typeof chunk.text === 'string') out += chunk.text
    }
    out = out.trim()
    if (!out) throw new Error('the model returned an empty reply')
    return out
  }

  history(limit) {
    const agent = this.agent
    const events = typeof agent?.session?.snapshotEvents === 'function' ? agent.session.snapshotEvents() : null
    if (!events) return null
    const messages = []
    for (const event of events) {
      const type = event?.type
      if (type === 'user/message') {
        const text = extractUserText(event)
        if (text) messages.push({ role: 'user', text })
      } else if (type === 'assistant/message') {
        const text = extractAssistantText(event)
        if (text) messages.push({ role: 'assistant', text })
      }
    }
    return messages.slice(-Math.max(1, limit))
  }

  describe() {
    return {
      session: this.sessionId,
      turns: this.turns,
      lastUsed: this.lastUsed ? new Date(this.lastUsed).toISOString() : null,
      agent: this.agent ? 'attached' : this.unavailable ? 'unavailable' : 'idle',
      ...(this.unavailable ? { reason: this.unavailable } : {}),
    }
  }
}

/** Text of a `user/message` event, whichever shape this build stores it in. */
function extractUserText(event) {
  const raw = event?.data?.message ?? event?.message ?? event?.payload?.message
  const content = raw?.content
  if (!Array.isArray(content)) return ''
  return content
    .filter((part) => part && part.type === 'text' && typeof part.text === 'string')
    .map((part) => part.text)
    .join('\n')
    .trim()
}

/** The tools this server advertises. Descriptions are part of the contract. */
function toolDefinitions() {
  return [
    {
      name: 'peer_ask',
      description:
        'Send a message to an agent running on the peer DSH host and return its final reply. ' +
        'The peer agent keeps its own tools, memory and permissions, so this is a request to ' +
        'another agent rather than a remote shell. Reuse the same `session` name to continue ' +
        'one conversation; use a new name to start a fresh one. For work that may outlast a ' +
        'comfortable wait, pass mode:"async" to get a task id back immediately and read the ' +
        'result with peer_task.',
      inputSchema: {
        type: 'object',
        properties: {
          prompt: { type: 'string', description: 'What to say to the peer agent.' },
          session: {
            type: 'string',
            description: 'Conversation slot on the peer host (default "default"). Reuse it to keep context.',
          },
          timeoutMs: {
            type: 'number',
            description: 'How long to wait for the reply, in milliseconds (sync mode only).',
          },
          mode: {
            type: 'string',
            enum: ['sync', 'async', 'poll'],
            description:
              'sync (default) waits for the reply; async returns a task id at once; poll reads a task by id.',
          },
          taskId: { type: 'string', description: 'Task id to read when mode is "poll".' },
          idempotencyKey: {
            type: 'string',
            description: 'Repeating the same key returns the earlier task instead of starting a second one.',
          },
        },
        required: ['prompt'],
        additionalProperties: false,
      },
    },
    {
      name: 'peer_task',
      description:
        'Read one background peer ask by task id. Waits up to `waitMs` for it to finish, so a caller can ' +
        'park a long task with peer_ask{mode:"async"} and then block only as long as it wants.',
      inputSchema: {
        type: 'object',
        properties: {
          taskId: { type: 'string', description: 'Task id returned by peer_ask{mode:"async"}.' },
          waitMs: { type: 'number', description: 'Longest time to wait for completion (default 0, max 300000).' },
        },
        required: ['taskId'],
        additionalProperties: false,
      },
    },
    {
      name: 'peer_sessions',
      description: 'List the conversation slots this peer host has handled, with turn counts and last use.',
      inputSchema: { type: 'object', properties: {}, additionalProperties: false },
    },
    {
      name: 'peer_history',
      description: 'Replay the most recent user and assistant messages of one conversation slot.',
      inputSchema: {
        type: 'object',
        properties: {
          session: { type: 'string', description: 'Conversation slot to read (default "default").' },
          limit: { type: 'number', description: `How many messages to return (default 10, max ${MAX_HISTORY}).` },
        },
        additionalProperties: false,
      },
    },
    {
      name: 'peer_status',
      description: 'Report the peer host, its model selection, and whether real agent sessions are available.',
      inputSchema: { type: 'object', properties: {}, additionalProperties: false },
    },
  ]
}

/** Bounded JSON body reader; returns null when the payload is not an object. */
async function readJsonBody(req) {
  const chunks = []
  let size = 0
  for await (const chunk of req) {
    size += chunk.length
    if (size > MAX_BODY_BYTES) throw new Error('request body too large')
    chunks.push(chunk)
  }
  if (size === 0) return null
  const text = Buffer.concat(chunks).toString('utf8')
  const parsed = JSON.parse(text)
  return parsed
}

function rpcResult(id, result) {
  return { jsonrpc: '2.0', id, result }
}

function rpcError(id, code, message, data) {
  return { jsonrpc: '2.0', id, error: { code, message, ...(data === undefined ? {} : { data }) } }
}

/** Text payload for a tool call: MCP allows plain content blocks only. */
function toolText(text) {
  return { content: [{ type: 'text', text }] }
}

export const name = SERVER_NAME

export function apply(ctx, config = {}) {
  const port = Number(config.port ?? DEFAULT_PORT)
  const stateKey = stateKeyFor(port)
  // A token can come from config, or from a file so the profile patch stays
  // free of secrets. Read once at activation: rotate by editing the file and
  // re-activating the plugin.
  let token = typeof config.token === 'string' ? config.token : ''
  const tokenFile = typeof config.tokenFile === 'string' ? config.tokenFile : ''
  if (!token && tokenFile) {
    try {
      token = fs.readFileSync(tokenFile, 'utf8').trim()
    } catch (error) {
      token = ''
      console.error(`dsh-peer: cannot read tokenFile ${tokenFile}: ${error?.message ?? error}`)
    }
  }
  const hostName = typeof config.hostLabel === 'string' && config.hostLabel
    ? config.hostLabel
    : os.hostname()
  const cwd = typeof config.cwd === 'string' && config.cwd ? config.cwd : process.cwd()
  const agentPreset = typeof config.agentPreset === 'string' ? config.agentPreset : ''
  // Session-level sandbox override for peer sessions. Leave empty to inherit the
  // deployment policy; set it on hosts whose sandbox backend cannot enforce the
  // default mode (otherwise every confined command is refused).
  const sandboxMode = ['read-only', 'workspace-write', 'danger-full-access'].includes(config.sandboxMode)
    ? config.sandboxMode
    : ''
  // Who this host is inside the peer vocabulary (`from=pc|phone|…`). Empty keeps
  // envelopes off and leaves replies as plain text.
  const identity = typeof config.identity === 'string' && /^[a-z][a-z0-9_-]{0,31}$/.test(config.identity)
    ? config.identity
    : ''
  // Validate, deduplicate and stamp CHATv1 envelopes around peer traffic.
  const chatProtocol = config.chatProtocol !== false && identity !== ''
  const maxChunkChars = Number.isFinite(config.maxChunkChars) && Number(config.maxChunkChars) > 0
    ? Math.max(1000, Math.floor(Number(config.maxChunkChars)))
    : DEFAULT_MAX_CHUNK_CHARS
  const dedupeSize = Number.isFinite(config.dedupeSize) && Number(config.dedupeSize) > 0
    ? Math.floor(Number(config.dedupeSize))
    : 500
  // Async asks outlive any single tool call, so their ceiling is minutes, not
  // the 240s a caller can afford to block for.
  const taskTimeout = Number.isFinite(config.taskTimeoutMs) && Number(config.taskTimeoutMs) > 0
    ? Math.floor(Number(config.taskTimeoutMs))
    : DEFAULT_TASK_TIMEOUT_MS
  // Global tools to mask from peer agents (e.g. the reverse bridge client).
  // Unknown names are skipped after the agent mounts, so a missing MCP server
  // never breaks agent creation.
  const denyTools = Array.isArray(config.denyTools)
    ? config.denyTools.filter((name) => typeof name === 'string' && name.length > 0)
    : []
  const prefix = typeof config.sessionPrefix === 'string' && config.sessionPrefix
    ? config.sessionPrefix
    : 'peer'
  const defaultTimeout = Number(config.askTimeoutMs ?? DEFAULT_ASK_TIMEOUT_MS)
  const maxSlots = Number.isFinite(config.maxSlots) && Number(config.maxSlots) > 0
    ? Math.max(1, Math.floor(Number(config.maxSlots)))
    : DEFAULT_MAX_SLOTS
  const dataDir = process.env.DSH_PEER_DATA ?? path.join(os.homedir(), '.dsh', 'peer')
  const logFile = config.logFile ?? path.join(dataDir, 'peer.log')

  try {
    fs.mkdirSync(path.dirname(logFile), { recursive: true })
  } catch {
    /* best effort */
  }
  const log = (msg) => {
    const line = `[${new Date().toISOString()}] ${msg}\n`
    try {
      fs.appendFileSync(logFile, line)
    } catch {
      /* ignore */
    }
  }

  log(`apply() called port=${port} host=${hostName}`)

  // Re-apply during hot reload leaves the old listener behind otherwise.
  const previous = globalThis[stateKey]
  if (previous && typeof previous.close === 'function') {
    try {
      previous.close()
      log('closed previous server instance')
    } catch (err) {
      log(`close previous failed: ${err?.message ?? err}`)
    }
  }

  const host = {
    ctx,
    log,
    cwd,
    agentPreset,
    sandboxMode,
    identity,
    chatProtocol,
    maxChunkChars,
    denyTools,
    maxSlots,
    slots: new Map(),
  }

  /** Drop the least recently used slots once the cap is exceeded. */
  const evictSlots = () => {
    while (host.slots.size > host.maxSlots) {
      let victimId = null
      let victimAt = Number.POSITIVE_INFINITY
      for (const [id, slot] of host.slots) {
        if (victimId === null || slot.lastUsed < victimAt) {
          victimId = id
          victimAt = slot.lastUsed
        }
      }
      if (victimId === null) return
      const victim = host.slots.get(victimId)
      host.slots.delete(victimId)
      log(`slot: evicted ${victimId} (cap ${host.maxSlots})`)
      void victim.dispose()
    }
  }

  /** Slot for one peer-named conversation, created on first use. */
  const slotFor = (requested) => {
    const raw = typeof requested === 'string' && requested.trim() ? requested.trim() : 'default'
    // Slots become session ids, so keep them to the characters a session id
    // accepts no matter what the peer sent.
    const key = raw.replace(/[^A-Za-z0-9_-]/g, '-').slice(0, 40) || 'default'
    const sessionId = `${prefix}:${key}`
    let slot = host.slots.get(sessionId)
    if (!slot) {
      slot = new Slot({ host, sessionId })
      host.slots.set(sessionId, slot)
      log(`slot: created ${sessionId}`)
      evictSlots()
    }
    return slot
  }

  /* ----------------------------------------------------------------------- *
   * Envelope bookkeeping
   *
   * `seen` is the duplicate filter: a peer that retries the same `msg` id (a
   * dropped response, an agent that re-reads its own history) must not start a
   * second turn, and a cached reply is usually better than a second answer.
   * ----------------------------------------------------------------------- */
  const seen = new Map()
  const remember = (msgId, value) => {
    if (!msgId) return
    seen.set(msgId, value)
    while (seen.size > dedupeSize) {
      const oldest = seen.keys().next().value
      if (oldest === undefined) break
      seen.delete(oldest)
    }
  }

  /** Turn the inbound text into `{ body, inbound }` and reject duplicates. */
  const admit = (prompt) => {
    if (!chatProtocol) return { body: prompt, inbound: parsePeerEnvelope(prompt), duplicate: null, malformed: false }
    const parsed = parsePeerEnvelope(prompt)
    if (!parsed.hasEnvelope) {
      // No envelope means a human typed this: that is the protocol, not an error.
      return { body: prompt, inbound: { hasEnvelope: false, envelope: { from: 'user', msg: '', reply: '-' } }, duplicate: null, malformed: false }
    }
    if (parsed.problems.length > 0) {
      // Incomplete header: keep the words, say what was wrong.
      return {
        body: prompt,
        inbound: { hasEnvelope: false, envelope: { from: 'user', msg: '', reply: '-' } },
        duplicate: null,
        malformed: true,
        problems: parsed.problems,
      }
    }
    const prior = seen.get(parsed.envelope.msg)
    if (prior !== undefined) return { body: parsed.body, inbound: parsed, duplicate: prior }
    return { body: parsed.body, inbound: parsed, duplicate: null, malformed: false }
  }

  /** Wrap a reply so the peer knows who said it and what it answers. */
  const sealReply = (text, inbound) => {
    if (!chatProtocol) return text
    const parsed = parsePeerEnvelope(text)
    if (parsed.hasEnvelope && parsed.problems.length === 0) return text
    const replyTo = inbound?.envelope?.msg || '-'
    const chain = inbound?.envelope?.chain && inbound.envelope.chain !== '-' ? inbound.envelope.chain : '-'
    const parts = splitBody(text, maxChunkChars)
    return parts
      .map((body, index) => {
        const chunk = parts.length > 1 ? ` chunk=${index + 1}/${parts.length}` : ''
        const header = formatPeerEnvelope({
          from: identity,
          msg: newMsgId(identity),
          chain,
          reply: replyTo,
          chunk: chunk ? `${index + 1}/${parts.length}` : '',
        })
        return `${header}\n${body}`
      })
      .join('\n\n')
  }

  /* ----------------------------------------------------------------------- *
   * Async tasks
   *
   * A peer ask can legitimately take minutes (a build, a device install, a
   * human clicking twice). Blocking one tool call for that long blocks the
   * caller's whole turn, so `mode=async` parks the work behind an id and the
   * caller polls. State is written to `<dataDir>/tasks`, which makes a poll
   * survive a host restart even though the turn itself does not.
   * ----------------------------------------------------------------------- */
  const tasks = new Map()
  // One directory per port: two rows in the same profile (a private peer and a\n  // group chat) must not prune each other's task files.\n  const taskDir = path.join(dataDir, 'tasks', String(port))
  const taskPath = (id) => path.join(taskDir, `${id.replace(/[^A-Za-z0-9_-]/g, '-')}.json`)
  const saveTask = (task) => {
    try {
      fs.mkdirSync(taskDir, { recursive: true })
      fs.writeFileSync(taskPath(task.id), JSON.stringify(task, null, 2))
    } catch (err) {
      log(`task: cannot persist ${task.id}: ${err?.message ?? err}`)
    }
  }
  const pruneTasks = () => {
    if (tasks.size <= 200) return
    const finished = [...tasks.values()]
      .filter((task) => task.state !== 'running')
      .sort((a, b) => a.startedAt - b.startedAt)
    for (const task of finished.slice(0, tasks.size - 200)) tasks.delete(task.id)
  }
  /** Reload persisted tasks; a `running` row means this host restarted mid-flight. */
  const loadTasks = () => {
    let names = []
    try {
      names = fs.readdirSync(taskDir).filter((name) => name.endsWith('.json'))
    } catch {
      return
    }
    for (const name of names) {
      try {
        const task = JSON.parse(fs.readFileSync(path.join(taskDir, name), 'utf8'))
        if (!task || typeof task.id !== 'string') continue
        if (task.state === 'running') {
          task.state = 'unknown'
          task.error = 'the peer host restarted before this task finished; its session may still hold the reply'
          saveTask(task)
        }
        tasks.set(task.id, task)
      } catch {
        /* a half-written file is not worth failing the boot */
      }
    }
    if (tasks.size > 0) log(`task: restored ${tasks.size} task record(s)`)
  }
  loadTasks()

  const startTask = ({ prompt, session, timeoutMs, idempotencyKey, inbound, body }) => {
    if (idempotencyKey) {
      for (const task of tasks.values()) {
        if (task.idempotencyKey === idempotencyKey) return task
      }
    }
    const slot = slotFor(session)
    const id = `t-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`
    const task = {
      id,
      idempotencyKey: idempotencyKey ?? '',
      session: slot.sessionId,
      prompt: body.slice(0, 500),
      msgId: inbound?.envelope?.msg ?? '',
      state: 'running',
      startedAt: Date.now(),
      finishedAt: 0,
      reply: '',
      error: null,
    }
    tasks.set(id, task)
    pruneTasks()
    saveTask(task)
    log(`task: accepted ${id} on ${slot.sessionId}`)
    void (async () => {
      try {
        const reply = await slot.ask(prompt, timeoutMs)
        task.reply = reply
        task.state = 'done'
      } catch (err) {
        task.error = String(err?.message ?? err)
        task.state = 'failed'
      } finally {
        task.finishedAt = Date.now()
        saveTask(task)
        log(`task: ${task.state} ${id} in ${task.finishedAt - task.startedAt}ms`)
      }
    })()
    return task
  }

  const describeTask = (task) =>
    JSON.stringify(
      {
        v: 1,
        task_id: task.id,
        session: task.session,
        status: task.state,
        duration_ms: (task.finishedAt || Date.now()) - task.startedAt,
        msg_id: task.msgId || null,
        output: task.state === 'done' ? { reply: task.reply } : {},
        error: task.error ?? null,
        no_forward_ack: true,
      },
      null,
      2,
    )

  const pollTask = async (taskId, waitMs) => {
    const task = tasks.get(taskId)
    if (!task) throw new Error(`unknown task id: ${taskId}`)
    const deadline = Date.now() + Math.max(0, Math.min(Number(waitMs) || 0, 300000))
    while (task.state === 'running' && Date.now() < deadline) {
      await new Promise((resolve) => setTimeout(resolve, 200))
    }
    return describeTask(task)
  }

  /** One JSON-RPC request → one JSON-RPC response (or null for notifications). */
  const handleMessage = async (message, state) => {
    if (!message || typeof message !== 'object') return rpcError(null, -32600, 'Invalid Request')
    const { id, method, params } = message
    const isNotification = id === undefined || id === null

    if (!method) {
      // A response from the client: nothing to answer.
      return null
    }

    switch (method) {
      case 'initialize': {
        const asked = params?.protocolVersion
        const protocolVersion = SUPPORTED_PROTOCOLS.includes(asked) ? asked : LATEST_PROTOCOL
        state.session = state.session ?? crypto.randomUUID()
        log(`initialize: client=${params?.clientInfo?.name ?? '?'} protocol=${protocolVersion}`)
        return rpcResult(id, {
          protocolVersion,
          capabilities: { tools: { listChanged: false } },
          serverInfo: { name: SERVER_NAME, version: VERSION },
          instructions:
            `Agents running on ${hostName} (a DSH host) are reachable through these tools. ` +
            'peer_ask sends a request to that agent; it answers with its own tools and context.',
        })
      }
      case 'notifications/initialized':
      case 'initialized':
        return null
      case 'ping':
        return rpcResult(id, {})
      case 'tools/list':
        return rpcResult(id, { tools: toolDefinitions() })
      case 'tools/call': {
        const toolName = params?.name
        const args = params?.arguments && typeof params.arguments === 'object' ? params.arguments : {}
        log(`tools/call ${toolName} ${JSON.stringify(Object.keys(args))}`)
        try {
          const text = await callTool(toolName, args)
          return rpcResult(id, toolText(text))
        } catch (err) {
          log(`tools/call ${toolName} failed: ${err?.message ?? err}`)
          // Tool failures are results, not protocol errors: the peer agent
          // needs to read what went wrong and decide what to do next.
          return rpcResult(id, { content: [{ type: 'text', text: `error: ${err?.message ?? err}` }], isError: true })
        }
      }
      case 'resources/list':
        return rpcResult(id, { resources: [] })
      case 'prompts/list':
        return rpcResult(id, { prompts: [] })
      default:
        if (isNotification) return null
        return rpcError(id, -32601, `Method not found: ${method}`)
    }
  }

  const callTool = async (toolName, args) => {
    switch (toolName) {
      case 'peer_ask': {
        const prompt = typeof args.prompt === 'string' ? args.prompt : ''
        if (!prompt.trim()) throw new Error('`prompt` must be a non-empty string')
        const mode = ['sync', 'async', 'poll'].includes(args.mode) ? args.mode : 'sync'
        if (mode === 'poll') {
          const taskId = typeof args.taskId === 'string' ? args.taskId.trim() : ''
          if (!taskId) throw new Error('mode "poll" needs a `taskId`')
          return pollTask(taskId, args.timeoutMs)
        }
        const timeoutMs = Number.isFinite(args.timeoutMs) ? Number(args.timeoutMs) : defaultTimeout
        const admitted = admit(prompt)
        if (admitted.duplicate) {
          log(`ask: duplicate msg ${admitted.inbound.envelope.msg} ignored`)
          return sealReply(
            JSON.stringify(
              {
                v: 1,
                status: 'done',
                duplicate: true,
                msg_id: admitted.inbound.envelope.msg,
                output: { reply: admitted.duplicate },
                no_forward_ack: true,
              },
              null,
              2,
            ),
            admitted.inbound,
          )
        }
        const note = admitted.malformed
          ? `\n\n[peer] this message had an incomplete CHATv1 header (${admitted.problems.join(', ')}); treated as a user line.\n`
          : ''
        const outbound = admitted.malformed ? `${prompt}${note}` : admitted.body
        if (mode === 'async') {
          const task = startTask({
            prompt: outbound,
            session: args.session,
            timeoutMs: Number.isFinite(args.timeoutMs) ? Number(args.timeoutMs) : taskTimeout,
            idempotencyKey: typeof args.idempotencyKey === 'string' ? args.idempotencyKey : '',
            inbound: admitted.inbound,
            body: outbound,
          })
          return JSON.stringify(
            {
              v: 1,
              status: 'accepted',
              task_id: task.id,
              session: task.session,
              msg_id: admitted.inbound?.envelope?.msg ?? null,
              hint: 'call peer_task with this task_id (waitMs optional) to read the result',
            },
            null,
            2,
          )
        }
        const reply = await slotFor(args.session).ask(outbound, timeoutMs)
        if (admitted.inbound?.envelope?.msg) remember(admitted.inbound.envelope.msg, reply)
        return sealReply(reply, admitted.inbound)
      }
      case 'peer_task': {
        const taskId = typeof args.taskId === 'string' ? args.taskId.trim() : ''
        if (!taskId) throw new Error('`taskId` must be a non-empty string')
        return pollTask(taskId, args.waitMs)
      }
      case 'peer_sessions': {
        const sessions = [...host.slots.values()].map((slot) => slot.describe())
        if (sessions.length === 0) return 'No conversation slots yet: call peer_ask to create one.'
        return JSON.stringify({ host: hostName, sessions }, null, 2)
      }
      case 'peer_history': {
        const limit = Math.min(MAX_HISTORY, Math.max(1, Number(args.limit ?? 10)))
        const slot = slotFor(args.session)
        const messages = slot.history(limit)
        if (messages === null) {
          return `Session ${slot.sessionId} has no live agent handle yet, so there is no history to read.`
        }
        if (messages.length === 0) return `Session ${slot.sessionId} has no messages yet.`
        return messages.map((m) => `${m.role}: ${m.text}`).join('\n\n')
      }
      case 'peer_status': {
        const defaults = (() => {
          try {
            return ctx.get('agentDefaultModel')?.currentSelection?.() ?? {}
          } catch {
            return {}
          }
        })()
        return JSON.stringify(
          {
            host: hostName,
            platform: `${process.platform}/${process.arch}`,
            pid: process.pid,
            cwd,
            agentPreset: agentPreset || '(registry default)',
            sandboxMode: sandboxMode || '(deployment default)',
            identity: identity || '(unset)',
            chatProtocol,
            maxChunkChars,
            tasks: [...tasks.values()].map((task) => ({
              id: task.id,
              state: task.state,
              session: task.session,
              startedAt: task.startedAt,
            })),
            model: defaults,
            endpoint: `http://0.0.0.0:${port}/mcp`,
            sessions: [...host.slots.values()].map((slot) => slot.describe()),
          },
          null,
          2,
        )
      }
      default:
        throw new Error(`unknown tool: ${toolName}`)
    }
  }

  const server = http.createServer(async (req, res) => {
    const url = new URL(req.url ?? '/', 'http://localhost')
    const caller = (req.socket.remoteAddress ?? '?').replace(/^::ffff:/, '')
    try {
      if (url.pathname === '/health') {
        res.writeHead(200, { 'Content-Type': 'application/json' })
        res.end(
          JSON.stringify({
            ok: true,
            service: SERVER_NAME,
            version: VERSION,
            host: hostName,
            platform: `${process.platform}/${process.arch}`,
            port,
            tools: toolDefinitions().map((tool) => tool.name),
            sessions: host.slots.size,
            maxSlots,
            token: token !== '',
            denyTools,
            sandboxMode: sandboxMode || null,
            identity: identity || null,
            chatProtocol,
            maxChunkChars,
            taskTimeoutMs: taskTimeout,
            tasks: {
              known: tasks.size,
              running: [...tasks.values()].filter((task) => task.state === 'running').length,
            },
          }),
        )
        return
      }

      if (url.pathname !== '/mcp') {
        res.writeHead(404, { 'Content-Type': 'application/json' })
        res.end(JSON.stringify({ ok: false, error: 'not found; the MCP endpoint is /mcp' }))
        return
      }

      if (token) {
        const header = String(req.headers.authorization ?? '')
        const presented = header.startsWith('Bearer ') ? header.slice(7).trim() : ''
        if (presented !== token) {
          log(`mcp: rejected unauthenticated ${req.method} from ${caller}`)
          res.writeHead(401, { 'Content-Type': 'application/json', 'WWW-Authenticate': 'Bearer' })
          res.end(JSON.stringify({ ok: false, error: 'unauthorized' }))
          return
        }
      }

      // No server-initiated stream: 405 is the spec's way of saying so, and the
      // client then keeps using plain POST responses.
      if (req.method === 'GET') {
        res.writeHead(405, { Allow: 'POST, DELETE' })
        res.end()
        return
      }
      if (req.method === 'DELETE') {
        res.writeHead(204)
        res.end()
        return
      }
      if (req.method !== 'POST') {
        res.writeHead(405, { Allow: 'POST, DELETE' })
        res.end()
        return
      }

      const payload = await readJsonBody(req)
      const state = { session: undefined }
      const incoming = Array.isArray(payload) ? payload : [payload]
      const responses = []
      for (const message of incoming) {
        const response = await handleMessage(message, state)
        if (response) responses.push(response)
      }

      const headers = { 'Content-Type': 'application/json' }
      if (state.session) headers['Mcp-Session-Id'] = state.session

      if (responses.length === 0) {
        // Notifications get an empty 202: there is nothing to send back.
        res.writeHead(202, headers)
        res.end()
        return
      }
      res.writeHead(200, headers)
      res.end(JSON.stringify(Array.isArray(payload) ? responses : responses[0]))
    } catch (err) {
      log(`mcp: ${req.method} ${url.pathname} failed: ${err?.message ?? err}`)
      if (!res.headersSent) {
        res.writeHead(400, { 'Content-Type': 'application/json' })
        res.end(JSON.stringify(rpcError(null, -32700, `parse error: ${err?.message ?? err}`)))
      } else {
        try {
          res.end()
        } catch {
          /* already gone */
        }
      }
    }
  })

  let legacyRetried = false
  server.on('error', (err) => {
    const message = String(err?.message ?? err)
    log(`server error: ${message}`)
    if (legacyRetried || !/EADDRINUSE/.test(message)) return
    legacyRetried = true
    // Builds before the per-port registry kept one listener under a port-less
    // key, so the registry above cannot close it. Free that orphan and take the
    // port back instead of dying on a stale bind.
    const legacyKey = Symbol.for('dsh-peer.server')
    const legacy = globalThis[legacyKey]
    if (legacy && typeof legacy.close === 'function') {
      try {
        legacy.close()
        delete globalThis[legacyKey]
        log('closed legacy (port-less) server instance')
      } catch (legacyErr) {
        log(`close legacy failed: ${legacyErr?.message ?? legacyErr}`)
      }
    }
    setTimeout(() => {
      try {
        server.listen(port, '0.0.0.0', () => {
          log(`listening on http://0.0.0.0:${port}/mcp (after retry)`)
        })
      } catch (retryErr) {
        log(`listen retry failed: ${retryErr?.message ?? retryErr}`)
      }
    }, 300)
  })
  server.listen(port, '0.0.0.0', () => {
    log(`listening on http://0.0.0.0:${port}/mcp`)
  })

  const close = () => {
    try {
      server.close()
    } catch {
      /* already closed */
    }
  }
  globalThis[stateKey] = { close }

  if (typeof ctx.on === 'function') {
    ctx.on('dispose', () => {
      log('dispose: closing listener')
      close()
      if (globalThis[stateKey]?.close === close) delete globalThis[stateKey]
      // Release the peer agents too, otherwise an unload leaves their session
      // locks and event logs behind inside the host.
      const leftovers = [...host.slots.values()]
      host.slots.clear()
      for (const slot of leftovers) void slot.dispose()
    })
  }

  return { close }
}

export { parsePeerEnvelope, formatPeerEnvelope, splitBody, newMsgId }
