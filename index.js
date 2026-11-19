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
/** Ceiling for one artifact download while validating a handoff. */
const DEFAULT_ARTIFACT_MAX_BYTES = 64 * 1024 * 1024
/** Per-request budget for HEAD/GET probes against an artifact URL. */
const DEFAULT_ARTIFACT_TIMEOUT_MS = 20000
/** Announced in capabilities so a caller can tell which envelope dialect is live. */
const CHAT_PROTOCOL_TAG = 'chatv1.1'

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
          require: {
            type: 'object',
            description:
              'Preflight checks against the peer capabilities. Unmet entries fail the call without spending a turn. ' +
              'Shape: {tools?: string[], sandbox?: string, workdir?: string, artifacts?: boolean, async?: boolean}.',
            properties: {
              tools: { type: 'array', items: { type: 'string' } },
              sandbox: { type: 'string' },
              workdir: { type: 'string' },
              artifacts: { type: 'boolean' },
              async: { type: 'boolean' },
            },
            additionalProperties: false,
          },
          artifacts: {
            type: 'array',
            description:
              'Files handed to the peer. Each entry is checked (HEAD, size, sha256) before the agent runs; ' +
              'a failure returns status "failed" with the problems instead of a silent bad path.',
            items: {
              type: 'object',
              properties: {
                role: { type: 'string' },
                url: { type: 'string' },
                path: { type: 'string' },
                sha256: { type: 'string' },
                size: { type: 'number' },
              },
              additionalProperties: false,
            },
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
    {
      name: 'room_say',
      description:
        'Post one message into a group room so every member can read it. Members read with room_sync; ' +
        'address `to` when the message is meant for one member, and `replyTo` when it answers an earlier seq.',
      inputSchema: {
        type: 'object',
        properties: {
          room: { type: 'string', description: 'Room name configured on this hub.' },
          text: { type: 'string', description: 'Message body.' },
          to: { type: 'string', description: 'Optional member identity to address directly.' },
          replyTo: { type: 'number', description: 'Optional seq this message answers.' },
        },
        required: ['room', 'text'],
        additionalProperties: false,
      },
    },
    {
      name: 'room_sync',
      description:
        'Read the events of a room after a sequence number. Pass waitMs to long-poll: the call returns as soon as ' +
        'something new arrives, or when the budget runs out (then count is 0).',
      inputSchema: {
        type: 'object',
        properties: {
          room: { type: 'string', description: 'Room name.' },
          since: { type: 'number', description: 'Last seq this member has already seen (default 0 = from the start).' },
          waitMs: { type: 'number', description: 'Longest wait for new events, in milliseconds (max 120000).' },
          ack: { type: 'boolean', description: 'Mark what is returned as read for the calling member (default: delivered only).' },
        },
        required: ['room'],
        additionalProperties: false,
      },
    },
    {
      name: 'room_pending',
      description:
        'List the messages this member still owes a read receipt for, oldest first. This is the redelivery view: ' +
        'anything unread comes back here no matter how many times the member reconnected.',
      inputSchema: {
        type: 'object',
        properties: {
          room: { type: 'string', description: 'Room name.' },
          identity: { type: 'string', description: 'Read on behalf of this member (hub operators only).' },
        },
        required: ['room'],
        additionalProperties: false,
      },
    },
    {
      name: 'room_ack',
      description: 'Acknowledge (mark read) every message up to `seq` for the calling member.',
      inputSchema: {
        type: 'object',
        properties: {
          room: { type: 'string', description: 'Room name.' },
          seq: { type: 'number', description: 'Highest seq to acknowledge (default: the room head).' },
        },
        required: ['room'],
        additionalProperties: false,
      },
    },
    {
      name: 'room_claim',
      description:
        'Take ownership of one message so two members do not both work on it. A second claim by another member ' +
        'fails with the current owner instead of silently forking the work.',
      inputSchema: {
        type: 'object',
        properties: {
          room: { type: 'string', description: 'Room name.' },
          seq: { type: 'number', description: 'Seq to own.' },
        },
        required: ['room', 'seq'],
        additionalProperties: false,
      },
    },
    {
      name: 'room_accept',
      description:
        'Sign off a message you own (or were addressed by): verdict "accepted" or "rejected", with an optional note. ' +
        'The verdict is recorded next to the message for good.',
      inputSchema: {
        type: 'object',
        properties: {
          room: { type: 'string', description: 'Room name.' },
          seq: { type: 'number', description: 'Seq being signed off.' },
          verdict: { type: 'string', enum: ['accepted', 'rejected'], description: 'Outcome.' },
          note: { type: 'string', description: 'Why, in one or two sentences.' },
        },
        required: ['room', 'seq', 'verdict'],
        additionalProperties: false,
      },
    },
    {
      name: 'room_list',
      description: 'List the rooms this hub hosts, with members, latest seq and the last message.',
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
  // Artifact handoff policy. A peer that sends `artifacts[]` gets each entry
  // checked (HEAD, content-length, sha256) before the agent ever sees it, so
  // "the file is on the share" stops being a claim taken on faith.
  const artifactsPolicy = {
    allowHosts: Array.isArray(config.artifactHosts)
      ? config.artifactHosts.filter((h) => typeof h === 'string' && h.length > 0)
      : [],
    maxBytes: Number.isFinite(config.artifactMaxBytes) && Number(config.artifactMaxBytes) > 0
      ? Math.floor(Number(config.artifactMaxBytes))
      : DEFAULT_ARTIFACT_MAX_BYTES,
    timeoutMs: Number.isFinite(config.artifactTimeoutMs) && Number(config.artifactTimeoutMs) > 0
      ? Math.floor(Number(config.artifactTimeoutMs))
      : DEFAULT_ARTIFACT_TIMEOUT_MS,
    // When true, any URL in the message body must also appear in artifacts[].
    strict: config.strictArtifacts === true,
  }
  // Deployment-declared abilities. Values left out stay null in `capabilities()`
  // so a caller can distinguish "cannot" from "did not say".
  const declaredCaps = config.capabilities && typeof config.capabilities === 'object' ? config.capabilities : {}

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

  /**
   * What this host promises it can do.
   *
   * Callers use this instead of guessing from the platform string: a Windows
   * box and an Android container are different in shell, sandbox and writable
   * roots, and none of that follows from "is it a phone".
   */
  const capabilities = () => {
    const declaredTools = Array.isArray(declaredCaps.tools)
      ? declaredCaps.tools.filter((tool) => typeof tool === 'string' && tool.length > 0)
      : null
    const declaredRoots = Array.isArray(declaredCaps.writableRoots)
      ? declaredCaps.writableRoots.filter((root) => typeof root === 'string' && root.length > 0)
      : [cwd, dataDir]
    const declaredShell = typeof declaredCaps.shell === 'string' && declaredCaps.shell
      ? declaredCaps.shell
      : process.platform === 'win32'
        ? 'pwsh'
        : 'bash'
    return {
      protocol: chatProtocol ? CHAT_PROTOCOL_TAG : 'plain',
      identity: identity || null,
      platform: `${process.platform}/${process.arch}`,
      cwd,
      shell: declaredShell,
      sandbox: sandboxMode || null,
      // null means "the deployment did not say", never "it has no tools".
      tools: declaredTools,
      writableRoots: declaredRoots,
      async: true,
      artifacts: {
        fetch: true,
        hosts: artifactsPolicy.allowHosts,
        maxBytes: artifactsPolicy.maxBytes,
        strict: artifactsPolicy.strict,
      },
      limits: {
        maxChunkChars,
        taskTimeoutMs: taskTimeout,
        askTimeoutMs: defaultTimeout,
        maxSlots,
      },
    }
  }

  /** Normalise the `require` argument into a value we can actually check. */
  const normalizeRequire = (raw) => {
    if (!raw || typeof raw !== 'object' || Array.isArray(raw)) return null
    const tools = Array.isArray(raw.tools)
      ? raw.tools.filter((tool) => typeof tool === 'string' && tool.length > 0)
      : []
    const sandbox = typeof raw.sandbox === 'string' ? raw.sandbox : ''
    const workdir = typeof raw.workdir === 'string' ? raw.workdir : ''
    const artifacts = raw.artifacts === true
    const wantsAsync = raw.async === true
    if (!tools.length && !sandbox && !workdir && !artifacts && !wantsAsync) return null
    return { tools, sandbox, workdir, artifacts, async: wantsAsync }
  }

  /**
   * Preflight a `require` client-side of the agent.
   *
   * Failing here costs nothing; failing inside the agent costs a turn and
   * usually ends in a `need_user` reply that the caller has to translate back
   * into "your host cannot do this".
   */
  const checkRequire = (wanted) => {
    const caps = capabilities()
    const missing = []
    const unverified = []
    if (wanted.sandbox) {
      if (!caps.sandbox) unverified.push(`sandbox:${wanted.sandbox} (host inherits its deployment policy)`)
      else if (caps.sandbox !== wanted.sandbox) missing.push(`sandbox:${wanted.sandbox} (host is ${caps.sandbox})`)
    }
    if (wanted.tools.length) {
      if (!caps.tools) {
        unverified.push(`tools:${wanted.tools.join(',')} (host declares no tool list)`)
      } else {
        const have = new Set(caps.tools)
        for (const tool of wanted.tools) if (!have.has(tool)) missing.push(`tool:${tool}`)
      }
    }
    if (wanted.async && caps.async !== true) missing.push('async')
    if (wanted.workdir && path.resolve(wanted.workdir) !== path.resolve(cwd)) {
      unverified.push(`workdir:${wanted.workdir} (host cwd is ${cwd})`)
    }
    return { ok: missing.length === 0, missing, unverified, capabilities: caps }
  }

  /** Keep only the artifact entries that carry a location. */
  const normalizeArtifacts = (raw) => {
    if (!Array.isArray(raw)) return []
    return raw
      .filter((item) => item && typeof item === 'object')
      .map((item) => ({
        role: typeof item.role === 'string' ? item.role : '',
        url: typeof item.url === 'string' ? item.url : '',
        file: typeof item.path === 'string' ? item.path : '',
        sha256: typeof item.sha256 === 'string' ? item.sha256.trim().toLowerCase() : '',
        size: Number.isFinite(item.size) ? Number(item.size) : null,
      }))
      .filter((item) => item.url || item.file)
      .slice(0, 32)
  }

  const hashFile = (file) =>
    new Promise((resolve, reject) => {
      const hash = crypto.createHash('sha256')
      const stream = fs.createReadStream(file)
      stream.on('error', reject)
      stream.on('data', (chunk) => hash.update(chunk))
      stream.on('end', () => resolve(hash.digest('hex')))
    })

  const fetchWithTimeout = async (url, init) => {
    const controller = new AbortController()
    const timer = setTimeout(() => controller.abort(), artifactsPolicy.timeoutMs)
    try {
      return await fetch(url, { ...init, signal: controller.signal })
    } finally {
      clearTimeout(timer)
    }
  }

  /** Hash a URL by streaming it, refusing anything over the byte budget. */
  const hashUrl = async (url) => {
    const response = await fetchWithTimeout(url, { method: 'GET' })
    if (!response.ok) throw new Error(`HTTP ${response.status}`)
    const hash = crypto.createHash('sha256')
    let total = 0
    for await (const chunk of response.body) {
      total += chunk.length
      if (total > artifactsPolicy.maxBytes) throw new Error(`artifact exceeds ${artifactsPolicy.maxBytes} bytes`)
      hash.update(chunk)
    }
    return { sha256: hash.digest('hex'), size: total }
  }

  /**
   * Check every declared artifact and return the ones worth trusting.
   *
   * A HEAD tells us the file exists and how big it is; a sha256 claim forces a
   * full GET. Failures are collected rather than thrown so one bad entry does
   * not hide the others.
   */
  const verifyArtifacts = async (list) => {
    const verified = []
    const problems = []
    for (const item of list) {
      const label = item.role || 'artifact'
      try {
        if (item.file) {
          const stat = fs.statSync(item.file)
          if (!stat.isFile()) throw new Error('not a regular file')
          if (item.size !== null && stat.size !== item.size) {
            throw new Error(`size ${stat.size} != declared ${item.size}`)
          }
          const digest = item.sha256 ? await hashFile(item.file) : null
          if (digest && digest !== item.sha256) throw new Error(`sha256 mismatch: got ${digest}`)
          verified.push({ role: label, path: item.file, sha256: digest ?? item.sha256 ?? null, size: stat.size })
          continue
        }
        const parsed = new URL(item.url)
        if (parsed.protocol !== 'http:' && parsed.protocol !== 'https:') {
          throw new Error(`unsupported protocol ${parsed.protocol}`)
        }
        if (artifactsPolicy.allowHosts.length > 0 && !artifactsPolicy.allowHosts.includes(parsed.hostname)) {
          throw new Error(`host ${parsed.hostname} is not in artifactHosts`)
        }
        let status = 0
        let declaredLength = null
        try {
          const head = await fetchWithTimeout(item.url, { method: 'HEAD' })
          status = head.status
          declaredLength = Number(head.headers.get('content-length') ?? '') || null
        } catch {
          // Some static servers answer HEAD with 405; fall through to the GET
          // path instead of treating the probe failure as a missing file.
          status = 0
        }
        let digest = null
        let size = declaredLength
        if (item.sha256 || status === 0 || status >= 400) {
          const full = await hashUrl(item.url)
          digest = full.sha256
          size = full.size
        }
        if (size !== null && item.size !== null && size !== item.size) {
          throw new Error(`size ${size} != declared ${item.size}`)
        }
        if (item.sha256 && digest && digest !== item.sha256) {
          throw new Error(`sha256 mismatch: got ${digest}`)
        }
        verified.push({ role: label, url: item.url, sha256: digest ?? item.sha256 ?? null, size })
      } catch (err) {
        problems.push({ role: label, url: item.url || null, path: item.file || null, error: String(err?.message ?? err) })
      }
    }
    return { verified, problems }
  }

  /** Write one handoff record so a transfer leaves a trace on this host. */
  const transfersDir = path.join(dataDir, 'transfers', String(port))
  const recordTransfer = (record) => {
    try {
      fs.mkdirSync(transfersDir, { recursive: true })
      const name = `${Date.now()}-${crypto.randomUUID().slice(0, 8)}.json`
      fs.writeFileSync(path.join(transfersDir, name), JSON.stringify(record, null, 2))
      const files = fs.readdirSync(transfersDir).filter((f) => f.endsWith('.json')).sort()
      for (const stale of files.slice(0, Math.max(0, files.length - 200))) {
        try {
          fs.unlinkSync(path.join(transfersDir, stale))
        } catch {
          /* best effort */
        }
      }
      return name
    } catch (err) {
      log(`transfer: record failed: ${err?.message ?? err}`)
      return null
    }
  }

  /** Pull the URLs a message body refers to, for the strict-artifact check. */
  const urlsInText = (text) => {
    const found = new Set()
    for (const match of String(text).matchAll(/https?:\/\/[^\s<>"')]+/g)) found.add(match[0])
    return [...found]
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

  const startTask = ({ prompt, session, timeoutMs, idempotencyKey, inbound, body, artifacts }) => {
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
      // Kept with the task so the caller can prove what was handed over long
      // after the message itself was compacted away.
      artifacts: Array.isArray(artifacts) ? artifacts : [],
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
        output: task.state === 'done' ? { reply: task.reply, artifacts: task.artifacts ?? [] } : {},
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

  // ---------------------------------------------------------------------------
  // Rooms: a group conversation that any number of agents can join.
  //
  // Topology is hub-and-spoke, deliberately. With N peers a mesh needs N(N-1)
  // client rows and N inbound ports; a hub needs one row per member and one
  // port. Members read with room_sync (a long-poll on a monotonic seq), so a
  // member behind NAT never has to accept a connection.
  //
  // Borrowed from: Matrix/Nostr (append-only event log with per-room sequence,
  // poll-since), MQTT (per-topic subscribers, at-least-once delivery), and
  // A2A (a message may address one member instead of the room).
  // ---------------------------------------------------------------------------
  const roomSpecs = config.rooms && typeof config.rooms === 'object' ? config.rooms : {}
  const roomMaxEvents = Number.isFinite(config.roomMaxEvents) && Number(config.roomMaxEvents) > 0
    ? Math.floor(Number(config.roomMaxEvents))
    : 2000
  const roomsDir = path.join(dataDir, 'rooms', String(port))
  const rooms = new Map()

  const roomPath = (name) => path.join(roomsDir, `${name}.jsonl`)

  const loadRoom = (name) => {
    const spec = roomSpecs[name]
    const members = Array.isArray(spec?.members)
      ? spec.members.filter((m) => typeof m === 'string' && m.length > 0)
      : []
    const room = { name, members, events: [], seq: 0, cursors: new Map() }
    try {
      const text = fs.readFileSync(roomPath(name), 'utf8')
      for (const line of text.split('\n')) {
        if (!line.trim()) continue
        try {
          const event = JSON.parse(line)
          room.events.push(event)
          room.seq = Math.max(room.seq, Number(event.seq) || 0)
        } catch {
          /* skip a torn line rather than losing the whole room */
        }
      }
    } catch {
      /* a room starts empty */
    }
    if (room.events.length > roomMaxEvents) {
      room.events = room.events.slice(-roomMaxEvents)
    }
    log(`room: loaded ${name} members=${members.join(',') || '-'} events=${room.events.length} seq=${room.seq}`)
    return room
  }

  const roomFor = (name) => {
    const key = typeof name === 'string' ? name.trim() : ''
    if (!key) throw new Error('`room` must be a non-empty string')
    if (!Object.prototype.hasOwnProperty.call(roomSpecs, key)) {
      const known = Object.keys(roomSpecs)
      throw new Error(`unknown room "${key}"${known.length ? `; configured: ${known.join(', ')}` : '; no rooms are configured'}`)
    }
    let room = rooms.get(key)
    if (!room) {
      room = loadRoom(key)
      rooms.set(key, room)
    }
    return room
  }

  const appendRoomEvent = (room, event) => {
    room.seq += 1
    const stored = { seq: room.seq, ...event }
    room.events.push(stored)
    try {
      fs.mkdirSync(roomsDir, { recursive: true })
      fs.appendFileSync(roomPath(room.name), `${JSON.stringify(stored)}\n`)
      if (room.events.length > roomMaxEvents * 1.5) {
        room.events = room.events.slice(-roomMaxEvents)
        fs.writeFileSync(roomPath(room.name), room.events.map((e) => `${JSON.stringify(e)}\n`).join(''))
        log(`room: compacted ${room.name} to ${room.events.length} events`)
      }
    } catch (err) {
      log(`room: write failed for ${room.name}: ${err?.message ?? err}`)
    }
    return stored
  }

  /**
   * Events after `since`, optionally waiting for new ones.
   *
   * The wait is a poll rather than a promise registry: a room is read by agents
   * that are already inside a turn, and a missed wake-up would hang that turn
   * for as long as the caller's budget. Polling has no such failure mode.
   */
  const readRoom = async (room, since, waitMs) => {
    const from = Number.isFinite(since) ? Number(since) : 0
    const deadline = Date.now() + Math.max(0, Math.min(Number(waitMs) || 0, 120000))
    let picked = room.events.filter((event) => event.seq > from)
    while (picked.length === 0 && Date.now() < deadline) {
      await new Promise((resolve) => setTimeout(resolve, 150))
      picked = room.events.filter((event) => event.seq > from)
    }
    return picked
  }

  const roomSummary = (room) => ({
    room: room.name,
    members: room.members,
    seq: room.seq,
    events: room.events.length,
    last: room.events.length ? room.events[room.events.length - 1] : null,
  })

  // ---------------------------------------------------------------------------
  // Receipts, redelivery and ownership.
  //
  // A room that only appends cannot tell "nobody has read this yet" from "it was
  // read and ignored". Receipts close that gap per member and per message:
  //
  //   queued ──(member syncs)──▶ delivered ──(member acks)──▶ read
  //                                   │
  //                                   └──(member claims)──▶ owned ──▶ accepted | rejected
  //
  // Borrowed from: SMTP DSN / XMPP XEP-0184 (per-recipient delivery + read
  // receipts), MQTT QoS1 (redelivery until the subscriber acknowledges), and
  // Redis Streams PEL / SQS visibility (a message stays pending until acked, so
  // an offline member is redelivered everything it missed).
  // ---------------------------------------------------------------------------
  const receiptsByRoom = new Map()
  const receiptsDir = roomsDir

  const receiptsPath = (name) => path.join(receiptsDir, `${name}.receipts.json`)

  const loadReceipts = (name) => {
    if (receiptsByRoom.has(name)) return receiptsByRoom.get(name)
    let map = {}
    try {
      map = JSON.parse(fs.readFileSync(receiptsPath(name), 'utf8'))
    } catch {
      map = {}
    }
    receiptsByRoom.set(name, map)
    return map
  }

  const saveReceipts = (name, map) => {
    try {
      fs.mkdirSync(receiptsDir, { recursive: true })
      fs.writeFileSync(receiptsPath(name), JSON.stringify(map))
    } catch (err) {
      log(`receipts: write failed for ${name}: ${err?.message ?? err}`)
    }
  }

  /** Receipt record for one seq, created on first look. */
  const receiptFor = (name, seq) => {
    const map = loadReceipts(name)
    const key = String(seq)
    if (!map[key]) map[key] = { delivered: {}, read: {}, owner: null, ownedAt: 0, verdict: null, acceptedBy: null, note: null }
    return map[key]
  }

  /** Who a message is for: its `to`, or every member except the sender. */
  const audienceOf = (room, event) => {
    if (event.to) return [event.to]
    const others = room.members.filter((member) => member !== event.from)
    return others.length > 0 ? others : [event.from]
  }

  /** Mark everything up to `seq` delivered/read for one member. */
  const markSeen = (room, member, upto, { read = false } = {}) => {
    const now = new Date().toISOString()
    let touched = 0
    for (const event of room.events) {
      if (event.seq > upto) break
      if (event.from === member) continue
      if (!audienceOf(room, event).includes(member)) continue
      const record = receiptFor(room.name, event.seq)
      if (!record.delivered[member]) {
        record.delivered[member] = now
        touched += 1
      }
      if (read && !record.read[member]) {
        record.read[member] = now
        touched += 1
      }
    }
    if (touched > 0) saveReceipts(room.name, loadReceipts(room.name))
    return touched
  }

  /** Everything this member still owes an answer for, oldest first. */
  const pendingFor = (room, member) =>
    room.events
      .filter((event) => event.seq > 0 && event.from !== member && audienceOf(room, event).includes(member))
      .map((event) => ({ event, receipt: receiptFor(room.name, event.seq) }))
      .filter(({ receipt }) => !receipt.read[member])
      .map(({ event, receipt }) => ({
        seq: event.seq,
        msg: event.msg,
        from: event.from,
        to: event.to,
        ts: event.ts,
        text: event.text,
        delivered: receipt.delivered[member] ?? null,
        owner: receipt.owner,
        verdict: receipt.verdict,
      }))

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
          const text = await callTool(toolName, args, state?.callerIdentity ?? '')
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

  const callTool = async (toolName, args, callerIdentity = '') => {
    // A member behind the hub speaks with its own identity header, so the room
    // log attributes the line to the sender instead of the hub operator.
    const speaker = /^[a-z][a-z0-9_-]{0,31}$/.test(callerIdentity) ? callerIdentity : identity
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
        // Preflight first: a host that cannot satisfy `require` answers at once
        // instead of spending a turn discovering it cannot.
        const requirements = normalizeRequire(args.require)
        if (requirements) {
          const check = checkRequire(requirements)
          if (!check.ok) {
            log(`ask: refused require=${JSON.stringify(requirements)} missing=${check.missing.join(',')}`)
            return JSON.stringify(
              {
                v: 1,
                status: 'failed',
                task_id: null,
                duration_ms: 0,
                msg_id: null,
                output: { missing: check.missing, unverified: check.unverified, capabilities: check.capabilities },
                evidence: [],
                error: 'requirements not met',
                no_forward_ack: true,
              },
              null,
              2,
            )
          }
        }
        // Verify declared artifacts before the agent runs, then hand it a list it
        // can trust instead of a path it has to hope about.
        const artifactList = normalizeArtifacts(args.artifacts)
        let artifactBlock = ''
        let artifactSummary = []
        if (artifactList.length) {
          const checked = await verifyArtifacts(artifactList)
          const record = recordTransfer({
            at: new Date().toISOString(),
            session: typeof args.session === 'string' ? args.session : 'default',
            direction: 'in',
            artifacts: checked.verified,
            problems: checked.problems,
          })
          if (checked.problems.length) {
            log(`ask: artifact verification failed (${checked.problems.length}) record=${record}`)
            return JSON.stringify(
              {
                v: 1,
                status: 'failed',
                task_id: null,
                duration_ms: 0,
                msg_id: null,
                output: { problems: checked.problems, record },
                evidence: [],
                error: 'artifact verification failed',
                no_forward_ack: true,
              },
              null,
              2,
            )
          }
          artifactSummary = checked.verified
          artifactBlock = `[peer] verified artifacts (checked before delivery):\n${checked.verified
            .map((a) => `- role=${a.role} source=${a.url ?? a.path} sha256=${a.sha256 ?? 'n/a'} size=${a.size ?? 'n/a'}`)
            .join('\n')}\n\n`
          log(`ask: ${checked.verified.length} artifact(s) verified, record=${record}`)
        }
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
        let outbound = admitted.malformed ? `${prompt}${note}` : admitted.body
        // Strict mode: every URL the body names must be one of the verified
        // artifacts. This is the "description is not delivery" rule.
        if (artifactsPolicy.strict || requirements?.artifacts === true) {
          const declared = new Set(artifactList.map((item) => item.url).filter(Boolean))
          const missingRefs = urlsInText(outbound).filter((url) => !declared.has(url))
          if (missingRefs.length) {
            log(`ask: strict artifacts refused ${missingRefs.length} undeclared url(s)`)
            return JSON.stringify(
              {
                v: 1,
                status: 'failed',
                task_id: null,
                duration_ms: 0,
                msg_id: null,
                output: { missing_artifacts: missingRefs, hint: 'pass each URL in artifacts[] with its sha256' },
                evidence: [],
                error: 'artifact references are not declared',
                no_forward_ack: true,
              },
              null,
              2,
            )
          }
        }
        if (artifactBlock) outbound = `${artifactBlock}${outbound}`
        if (mode === 'async') {
          const task = startTask({
            prompt: outbound,
            session: args.session,
            timeoutMs: Number.isFinite(args.timeoutMs) ? Number(args.timeoutMs) : taskTimeout,
            idempotencyKey: typeof args.idempotencyKey === 'string' ? args.idempotencyKey : '',
            inbound: admitted.inbound,
            body: outbound,
            artifacts: artifactSummary,
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
            capabilities: capabilities(),
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
      case 'room_say': {
        const room = roomFor(args.room)
        const text = typeof args.text === 'string' ? args.text : ''
        if (!text.trim()) throw new Error('`text` must be a non-empty string')
        const to = typeof args.to === 'string' ? args.to.trim() : ''
        if (to && room.members.length > 0 && !room.members.includes(to)) {
          throw new Error(`"${to}" is not a member of ${room.name} (members: ${room.members.join(', ')})`)
        }
        const event = appendRoomEvent(room, {
          msg: newMsgId(speaker || 'anonymous'),
          from: speaker || 'anonymous',
          ts: new Date().toISOString(),
          to: to || null,
          reply_to: Number.isFinite(args.replyTo) ? Number(args.replyTo) : null,
          text: text.slice(0, maxChunkChars),
        })
        log(`room: say ${room.name} seq=${event.seq} from=${event.from}${to ? ` to=${to}` : ''}`)
        return JSON.stringify(
          { v: 1, room: room.name, seq: event.seq, msg: event.msg, ts: event.ts, from: event.from, to: event.to },
          null,
          2,
        )
      }
      case 'room_sync': {
        const room = roomFor(args.room)
        const since = Number.isFinite(args.since) ? Number(args.since) : 0
        const events = await readRoom(room, since, args.waitMs)
        const member = args.ack === false ? '' : speaker
        let receipts = null
        if (member) {
          const upto = events.length > 0 ? events[events.length - 1].seq : since
          const updated = markSeen(room, member, upto, { read: args.ack === true })
          receipts = { member, acked: args.ack === true, updated }
        }
        return JSON.stringify(
          {
            v: 1,
            room: room.name,
            since,
            seq: room.seq,
            count: events.length,
            receipts,
            events,
          },
          null,
          2,
        )
      }
      case 'room_pending': {
        const room = roomFor(args.room)
        const member = /^[a-z][a-z0-9_-]{0,31}$/.test(String(args.identity ?? '')) ? String(args.identity) : speaker
        if (!member) throw new Error('room_pending needs an identity (call it with a member header)')
        const events = pendingFor(room, member)
        return JSON.stringify({ v: 1, room: room.name, member, count: events.length, pending: events }, null, 2)
      }
      case 'room_ack': {
        const room = roomFor(args.room)
        const member = speaker
        if (!member) throw new Error('room_ack needs an identity (call it with a member header)')
        const upto = Number.isFinite(args.seq) ? Number(args.seq) : room.seq
        const updated = markSeen(room, member, upto, { read: true })
        log(`room: ack ${room.name} upto=${upto} member=${member} updated=${updated}`)
        return JSON.stringify({ v: 1, room: room.name, member, seq: upto, updated }, null, 2)
      }
      case 'room_claim': {
        const room = roomFor(args.room)
        const seq = Number(args.seq)
        const event = room.events.find((item) => item.seq === seq)
        if (!event) throw new Error(`no message with seq ${seq} in ${room.name}`)
        const owner = speaker
        if (!owner) throw new Error('room_claim needs an identity (call it with a member header)')
        const receipt = receiptFor(room.name, seq)
        if (receipt.owner && receipt.owner !== owner) {
          throw new Error(`seq ${seq} is already owned by ${receipt.owner}`)
        }
        if (!receipt.delivered[owner]) receipt.delivered[owner] = new Date().toISOString()
        receipt.owner = owner
        receipt.ownedAt = Date.now()
        saveReceipts(room.name, loadReceipts(room.name))
        log(`room: claim ${room.name} seq=${seq} owner=${owner}`)
        return JSON.stringify({ v: 1, room: room.name, seq, owner, claimed_at: receipt.ownedAt }, null, 2)
      }
      case 'room_accept': {
        const room = roomFor(args.room)
        const seq = Number(args.seq)
        const verdict = args.verdict
        if (!['accepted', 'rejected'].includes(verdict)) throw new Error('`verdict` must be "accepted" or "rejected"')
        const event = room.events.find((item) => item.seq === seq)
        if (!event) throw new Error(`no message with seq ${seq} in ${room.name}`)
        const by = speaker
        if (!by) throw new Error('room_accept needs an identity (call it with a member header)')
        const receipt = receiptFor(room.name, seq)
        // Only the owner signs off, or the addressee when nobody has claimed it
        // yet — never an unrelated bystander.
        if (receipt.owner && receipt.owner !== by) {
          throw new Error(`seq ${seq} is owned by ${receipt.owner}; only the owner can accept or reject it`)
        }
        if (!receipt.owner) {
          if (!audienceOf(room, event).includes(by)) {
            throw new Error(`seq ${seq} is not addressed to ${by}`)
          }
          receipt.owner = by
          receipt.ownedAt = Date.now()
        }
        receipt.verdict = verdict
        receipt.acceptedBy = by
        receipt.note = typeof args.note === 'string' ? args.note.slice(0, 2000) : null
        receipt.acceptedAt = new Date().toISOString()
        if (!receipt.read[by]) receipt.read[by] = receipt.acceptedAt
        saveReceipts(room.name, loadReceipts(room.name))
        log(`room: accept ${room.name} seq=${seq} by=${by} verdict=${verdict}`)
        return JSON.stringify(
          { v: 1, room: room.name, seq, verdict, by, note: receipt.note, at: receipt.acceptedAt },
          null,
          2,
        )
      }
      case 'room_list': {
        return JSON.stringify(
          { rooms: Object.keys(roomSpecs).map((name) => roomSummary(roomFor(name))) },
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
            capabilities: capabilities(),
            rooms: Object.keys(roomSpecs),
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
      const state = {
        session: undefined,
        // Optional per-client identity: a member's MCP client row can send
        // `X-Peer-Identity: <name>` so room traffic is attributed to it rather
        // than to this host. The token is still what authorises the call.
        callerIdentity: String(req.headers['x-peer-identity'] ?? '').trim(),
      }
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
