# dsh-peer

Let two DeepSeek Harness installations talk to each other.

`dsh-peer` is a **zero-dependency MCP server** that exposes the local agent as five
MCP tools. Combined with the stock `@deepseek-ai/dsh-mcp-client`, it turns
"my laptop's agent" and "my phone's agent" into two peers that can ask each other
questions over the LAN — no cloud relay, no shared session file.

```
   PC (DSH desktop)                          Phone (DSH Go)
┌──────────────────────────┐             ┌──────────────────────────┐
│ agent                    │             │ agent                    │
│   │ tools                │             │   │ tools                │
│   ├─ @deepseek-ai/       │  MCP/HTTP   │   ├─ dsh-peer            │
│   │  dsh-mcp-client ─────┼────────────▶│   │  POST /mcp  :38404   │
│   │  (serverName: phone) │             │   │  → agent.session     │
│   └──────────────────────┘             │   └──────────────────────┘
│ dsh-peer  POST /mcp :38404 ◀───────────┼───── dsh-mcp-client      │
└──────────────────────────┘  MCP/HTTP   └──────────────────────────┘
```

Either side may run `dsh-peer`, either side may run the client, and one side can
do both (that is how the diagram above is wired).

## What you get

| Tool (raw name) | Purpose | Input |
| --- | --- | --- |
| `peer_ask` | Send a prompt to the peer's agent | `prompt`, `session?`, `timeoutMs?`, `mode?` (`sync`\|`async`\|`poll`), `taskId?`, `idempotencyKey?`, `require?`, `artifacts?` |
| `peer_task` | Wait for / read an async task | `taskId`, `waitMs?` (max 300 000) |
| `peer_sessions` | List conversation slots and their turn counts | – |
| `peer_history` | Read back the last messages of one slot | `session?`, `limit?` |
| `peer_status` | Host, platform, model, identity, sandbox mode, **capabilities**, task counters | – |
| `room_say` | Post one message into a group room | `room`, `text`, `to?`, `replyTo?` |
| `room_sync` | Read room events after a seq, or long-poll for new ones | `room`, `since?`, `waitMs?` (max 120 000) |
| `room_list` | List rooms with members, latest seq and last message | – |

Through `dsh-mcp-client` these become `mcp__<serverName>__peer_ask` and friends,
for example `mcp__phone__peer_ask` on the PC and `mcp__pc__peer_ask` on the phone.

Each `session` value is a **separate conversation** on the peer: the peer keeps
one DSH session per slot (`peer:default`, `peer:review`, …) so a review thread does
not inherit the small-talk context. Calls within one slot are serialised, different
slots run in parallel. Slots are evicted LRU-wise once `maxSlots` is reached.

## Chat protocol (CHATv1)

Two agents that both start with "you are a helpful assistant" cannot tell each
other from the human. Set `identity` on both sides and every message carries a
one-line envelope:

```
[[CHATv1 from=phone msg=phone-20261119T152819Z-tkwgad ts=2026-11-19T15:28:19Z chain=- reply=-]]
收到
```

- `from` — the sender's identity (`pc`, `phone`, …). No envelope at all means the
  message came from **the human**.
- `msg` — `<from>-<UTC stamp>-<base36 tail>`; send the same `msg` twice and the
  second call returns the cached answer with `"duplicate": true` instead of
  running another turn.
- `ts` — UTC, `Z`-suffixed.
- `chain` — who has forwarded the line so far (`pc>phone`), `-` for a direct line.
- `reply` — the `msg` this answers, `-` when it starts a thread.

Replies are sealed automatically (`from=<identity>`, `reply=<inbound msg>`), and a
reply longer than `maxChunkChars` is split into `chunk=1/3`, `2/3`, … envelopes.
An unparseable header is not thrown away: the text is treated as a human line and
the peer is told which fields were malformed.

This is a *convention*, not access control — the token is what authenticates.

## Capabilities, preflight and artifacts

Three rules that turn "I described the work" into "the work actually landed".

### 1. Ask what the peer can do

`peer_status` returns a `capabilities` object. Read it instead of guessing from
the platform string — a Windows box and an Android container differ in shell,
sandbox and writable roots, and none of that follows from "is it a phone":

```json
{
  "protocol": "chatv1.1",
  "identity": "phone",
  "platform": "linux/arm64",
  "cwd": "/root/workspace",
  "shell": "bash",
  "sandbox": "danger-full-access",
  "tools": ["bash", "read", "write", "edit"],
  "writableRoots": ["/root/workspace"],
  "async": true,
  "artifacts": { "fetch": true, "hosts": [], "maxBytes": 67108864, "strict": false },
  "limits": { "maxChunkChars": 60000, "taskTimeoutMs": 1800000, "askTimeoutMs": 240000, "maxSlots": 8 }
}
```

`tools: null` means the deployment did not declare a tool list; it never means
"no tools". Declare yours with the `capabilities` config key.

### 2. Preflight with `require`

Pass `require` and the peer answers before spending a turn:

```json
{ "prompt": "build and install the APK", "require": { "tools": ["bash"], "sandbox": "danger-full-access" } }
```

If the host cannot satisfy it, the call returns immediately:

```json
{ "v": 1, "status": "failed", "error": "requirements not met",
  "output": { "missing": ["sandbox:workspace-write (host is danger-full-access)"], "unverified": [], "capabilities": { … } } }
```

`unverified` is the third state: the host declares nothing, so it *might* be able
to do it. That is a report, not a refusal.

### 3. `artifacts[]`: verified, not described

```json
{
  "prompt": "take this file and resize it",
  "artifacts": [
    { "role": "input", "url": "http://192.0.2.10:38405/in.png", "sha256": "…", "size": 20480 }
  ]
}
```

Each entry is checked before the agent runs: `HEAD` for existence and length,
and a streamed `sha256` when a digest is given (bounded by `artifactMaxBytes`).
Failures return `status: "failed"` with the concrete reason per entry, e.g.
`sha256 mismatch: got 9f2c…`, `host example.org is not in artifactHosts`.

Successful checks are:

- prepended to the agent's prompt as a list it can trust;
- written to `<data dir>/transfers/<port>/<ts>-<rand>.json` (last 200 kept);
- attached to the task record, so `peer_task` shows them long after the message
  itself was compacted away.

Set `strictArtifacts: true` (or pass `require.artifacts: true`) and any URL named
in the body must also appear in `artifacts[]` — a bare "the file is at <url>" is
refused with `error: "artifact references are not declared"`.

## Group rooms (three or more agents)

Two agents can call each other. Three cannot do that without a hub: a mesh needs
`N(N-1)` client rows and `N` reachable ports. So one host is the **hub** and the
rest are members:

```
   pc ─────────┐
               ├── hub (rooms: lobby, build)  ← append-only log, one seq per room
   phone ──────┤
               │
   laptop ─────┘
```

Declare the rooms on the hub's config (an undeclared room is refused by name):

```yaml
rooms:
  lobby:
    members: [pc, phone, laptop]
  build:
    members: [pc, phone]
roomMaxEvents: 2000
```

Members need **one** client row pointing at the hub, then:

- `room_say { room, text, to?, replyTo? }` → `{ seq, msg, ts }`
- `room_sync { room, since, waitMs? }` → events after `since`; with `waitMs` it
  returns as soon as something arrives, so a member can block inside a turn
  instead of polling from outside.
- `room_list` → roster, latest seq, last message.

Design notes, borrowed from systems that solved this already:

- **Matrix / Nostr**: the log is the truth. Each room is a JSONL file with a
  monotonic `seq`, so a member's cursor is a single number and replay after a
  restart is exact.
- **MQTT**: delivery is at-least-once by *reading*, not by pushing. A member that
  was offline simply asks for everything after its cursor.
- **A2A**: a message is addressed to the room by default and to one member via
  `to`; membership is checked, so a whisper to a non-member is an error rather
  than a line nobody receives.
- **NAT**: nothing is pushed, so a member behind a phone hotspot never needs an
  inbound port — the hub is the only reachable side.

Rooms live at `<data dir>/rooms/<port>/<room>.jsonl` and are compacted back to
`roomMaxEvents` once the file grows past 1.5×.

## Long tasks

Old versions blocked the caller for as long as the peer needed (up to
`askTimeoutMs`, and a human approval could sit in the middle of it). Ask with
`mode: "async"` instead and you get an acknowledgement in milliseconds:

```json
{ "v": 1, "status": "accepted", "task_id": "t-mwnpqttf-7c3uel", "session": "peer:build" }
```

Then poll with `peer_task` (`waitMs` up to 300 000, repeatable) or
`peer_ask` + `mode: "poll"`. Tasks live up to `taskTimeoutMs` (default 30 min),
are persisted to `<data dir>/tasks/<port>/<id>.json` (one directory per instance),
and a task that was still `running` when the host died is reported as `unknown`
instead of hanging forever.

The peer's *own* tool timeouts still apply: the executor preset caps a single shell
command at 300 s, so a 10-minute job needs to be launched as a background process
the peer can poll, not slept inline.

Pass `idempotencyKey` when a retry must not start a second run — the same key
returns the same `task_id`.

## If the peer needs a human

When the remote agent asks for an approval, the plugin does not wait it out: the
call returns immediately with `status: "need_user"` and the pending request, so
the caller can surface it, wait, and ask again.

```json
{ "v": 1, "status": "need_user", "output": { "approval": { "type": "approval/asked", "toolName": "bash", "reason": "…" } } }
```

## Install

### On a desktop profile

```bash
npm pack                      # produces dsh-peer-0.2.2.tgz
dsh plugin --profile desktop install ./dsh-peer-0.2.2.tgz
```

or, from an agent that has the plugin manager:

```text
plugin_manager install_bundle target="file:C:\path\to\dsh-peer-0.2.2.tgz"
```

A bundle patch installs the server row (`id: dsh-peer`, `port: 38404`) and takes
effect without restarting the app. Re-installing a *different* version re-runs the
plugin with the new config; re-installing the *same* version is rejected as
`already-installed`.

### On DSH Go (phone)

Copy the package into the phone's plugin directory and register the bundle:

```bash
mkdir -p /root/.dsh/dsh-go/plugins/dsh-peer
tar xzf dsh-peer-0.2.2.tgz -C /tmp && cp -a /tmp/package/. /root/.dsh/dsh-go/plugins/dsh-peer/
node --check /root/.dsh/dsh-go/plugins/dsh-peer/index.js
```

Then in `/root/.dsh/profiles/web/package.json` add the dependency and the bundle:

```json
"dependencies": { "dsh-peer": "link:/root/.dsh/dsh-go/plugins/dsh-peer" },
"dsh": { "profile": { "bundles": ["...", "dsh-peer"] } }
```

**Two hard requirements on DSH Go** — both cost an afternoon if you miss them:

1. `link:` dependencies do **not** create the node_modules link. Do it yourself, or the
   app logs `dsh: skipping profile bundle "dsh-peer": cannot resolve profile bundle`:

   ```bash
   ln -sfn ../../../dsh-go/plugins/dsh-peer /root/.dsh/profiles/web/node_modules/dsh-peer
   ```

2. Anything you mount by package **name** (for example
   `@deepseek-ai/dsh-mcp-client`) must be resolvable from the *profile* directory.
   The Go runtime keeps its packages under `/opt/dsh/node_modules`, which Node never
   walks to, so link the packages you need:

   ```bash
   ln -sfn /opt/dsh/node_modules/@deepseek-ai/dsh-mcp-client \
          /root/.dsh/profiles/web/node_modules/@deepseek-ai/dsh-mcp-client
   ```

   Verify with `node -e "import('@deepseek-ai/dsh-mcp-client').then(()=>console.log('ok'))"`
   **run from the profile directory**.

### Wiring the client

Add one row to the profile's `cordis.patch.yml`. The patch file is a list of patch
operations, so a **new** entry must be wrapped in `insert:` — a bare `- id: my-row`
is rejected with `patch: entry "my-row" not found`:

```yaml
- insert:
    - id: peer-phone
      name: "@deepseek-ai/dsh-mcp-client"
      config:
        serverName: phone                 # → mcp__phone__* tools
        transport: streamable-http
        url: http://203.0.113.42:38404/mcp  # the peer's LAN address
        headers: { Authorization: "Bearer <the peer's token>" }
        toolCallTimeoutMs: 600000
```

A desktop install composes its patch list **at boot**; a fresh row needs an app
restart. DSH Go re-composes on every app start, so a phone-side restart is enough.
`dsh` is not always on `PATH`; when it is missing you can dump the composed config
with `dsh --profile <p> --dump-config` from the Go runtime instead.

### Two instances on one host

`stateKeyFor(port)` keeps each port's server in its own `globalThis` slot, so the
same package can back several rows at once (for example a private `peer` on 38404
and a group `pc-chat` on 38406 with its own identity, preset and token). An older
build that used a single port-less key would refuse the second bind with
`EADDRINUSE`; 0.1.6+ detects the legacy instance, closes it and rebinds.

## Configuration

| Key | Default | Meaning |
| --- | --- | --- |
| `port` | `38404` | HTTP port for `/mcp` |
| `host` | `0.0.0.0` | Bind address |
| `token` | `''` | Static bearer token |
| `tokenFile` | `''` | Read the token from a file (trimmed); wins over `token` |
| `identity` | `''` | CHATv1 name; empty disables the protocol |
| `chatProtocol` | `identity !== ''` | Seal/reply/dedupe envelopes |
| `maxChunkChars` | `60000` | Split longer replies into `chunk=i/n` |
| `dedupeSize` | `500` | Remembered `msg` ids per instance |
| `sessionPrefix` | `peer` | Slot ids are `<prefix>:<session>` |
| `agentPreset` | `''` | Preset for peer sessions; empty = profile default |
| `sandboxMode` | `''` | Pin `read-only` / `workspace-write` / `danger-full-access` per session |
| `denyTools` | `[]` | Tool names removed from peer sessions (loop guard) |
| `askTimeoutMs` | `240000` | Default sync wait inside `peer_ask` |
| `taskTimeoutMs` | `1800000` | Async task ceiling |
| `maxSlots` | `8` | LRU eviction threshold for conversation slots |
| `cwd` | `process.cwd()` | Working directory for peer sessions |
| `logFile` | `<data dir>/peer.log` | Append-only log of `health`, `initialize`, `tools/call` |

## Guarding against loops

Two peers that both expose `peer_ask` will happily call each other forever. Two
independent brakes:

- `denyTools: [mcp__phone__peer_ask, mcp__phone__peer_history, …]` removes the
  bridge tools from the peer's own agent, so it cannot call back.
- Run the peer's sessions under a preset that does not mount `dsh-mcp-client` at all
  (a dedicated executor preset with only file/shell tools).

Either one alone is enough for a polite peer; both are worth having.

## Verify a peer without any agent

`tools/mcp-probe.mjs` is a small MCP client with no dependencies:

```bash
PEER_TOKEN=<token> node tools/mcp-probe.mjs http://203.0.113.42:38404/mcp "介绍一下你自己"
```

It prints `initialize`, `tools/list`, `peer_status`, `peer_history`, a `peer_ask`
round-trip and the sessions table. For the async path, call `peer_ask` with
`mode: "async"` and follow up with `peer_task`.

`smoke.mjs` runs the same protocol surface against a throwaway server with a stub
context (`SMOKE_OK`), including envelope parsing, dedupe, chunking and the async
accept → poll handshake, so it works in CI where no model exists.

## Behaviour that matters

- **The peer's own approval policy applies.** If the remote agent decides to call a
  tool the user there must approve, `peer_ask` returns `need_user` immediately
  instead of blocking; ask again after it has been approved.
- **Some hosts cannot sandbox at all.** In a PRoot/container environment (DSH Go on
  Android, for instance) `mechanism` probes fail and every command is refused with
  `SANDBOX_UNAVAILABLE`. That is not an approval prompt — the only way forward is
  `sandboxMode: danger-full-access` on the peer row. Do that knowingly: the peer's
  agent then runs unconfined.
- **The peer answers with a fresh session per slot**, created through
  `agents.create` with the row's preset. If the peer cannot create an agent it falls
  back to a plain `llm.stream()` call, which has no tools and no memory — useful for
  a smoke test, not for work. A failed creation is retried after 30 s instead of
  downgrading for the rest of the process lifetime.
- **LAN addresses drift** (a DHCP lease today, something else after a reboot).
  Treat `url` as configuration: update it and restart, or give the host a DHCP
  reservation.
- **Security**: the server is trusted-LAN only by default. Anyone who can reach the
  port can drive an agent on your machine. Set `token`/`tokenFile` when the network
  is shared, and never expose the port to the internet. `/health` is unauthenticated
  on purpose (it is the discovery/status probe) and only reports counts and config.
- **One transport per connection.** `GET /mcp` answers `405` on purpose: this server
  sends no server-initiated stream, so the client must use plain request/response.

## License

MIT.
