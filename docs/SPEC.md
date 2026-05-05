# blemees-peer (`blemees-peerd`) — Peer communication daemon

**Version:** 0.1
**Protocol:** `peer/1`
**Language:** Python 3.11+, stdlib only
**Target OS:** Linux, macOS

A sibling to [`blemees-agentd`](https://github.com/blemees/blemees-agent).
`blemees-agentd` is the *operator* channel — humans drive agents through it.
`blemees-peerd` is the *peer* channel — agents talk to each other through it.
The two daemons are independent processes with independent lifecycles
and independent persistence; if `blemees-peerd` is down, `blemees-agentd` sessions
continue to work, just without peer messaging.

---

## 1. Architecture

```
                       ┌────────────────┐
                       │      TUI       │
                       └────────┬───────┘
                                │ blemees/2 (operator)
                       ┌────────▼───────┐
                       │ blemees-agentd │  spawns agents
                       └─┬────────────┬─┘
                         │            │
                         ▼            ▼
                  ┌──────────┐  ┌──────────┐
                  │ agent A  │  │ agent B  │  (MCP sidecar
                  │ (claude) │  │ (codex)  │   added via the
                  └────┬─────┘  └────┬─────┘   agent's own MCP
                       │ peer/1     │          config — see §9)
                       └─────┬──────┘
                             ▼
                       ┌──────────┐
                       │ blemees-peerd│  routes + persists
                       └──────────┘
```

`blemees-peerd` knows nothing about `blemees-agentd`, and `blemees-agentd` knows nothing
about `blemees-peerd`. The two systems are fully decoupled: the peer mesh
works whether or not agents were spawned by `blemees-agentd`, and `blemees-agentd`
works whether or not `blemees-peerd` is installed. The only things `blemees-peerd`
sees are peers identified by **session home** (the agent's working
directory at session spawn) and a session ID, gated by the Unix
socket permission of the user who owns it.

## 2. Identity

A peer is identified by **the agent's working directory at session
spawn**, normalized to a stable string. This is the session's *home*
in `blemees-peerd`'s vocabulary. Two sessions in the same home are co-located
peers; rationale: agents in the same project naturally collaborate.

### Path normalization

The agent's harness (e.g. `blemees-agentd`) resolves the path once at spawn
(`os.path.realpath`) and freezes it; agent `cd` mid-session does
**not** change peer identity. The canonical form is then:

- If the resolved path is at or under the user's home directory
  (`os.path.expanduser("~")`), it is rewritten with a leading `~/`
  (or just `~` for the home directory itself):
  - `/Users/me`             → `~`
  - `/Users/me/foo`         → `~/foo`
  - `/Users/me/projects/x`  → `~/projects/x`
- Otherwise it stays absolute:
  - `/tmp/scratch`          → `/tmp/scratch`
  - `/etc/something`        → `/etc/something`

Both the harness and `blemees-peerd` must apply this rule identically; the
normalized string is what appears in addresses and on disk.

### Aliases

A session can claim a short, human-meaningful identity string
("architect", "tester", "reviewer") via `peer.set_alias`. Aliases are:

- **Scoped to the home.** Two different homes may both have an
  `architect`; they are distinct addresses.
- **Unique within their home.** A `set_alias` whose alias is already
  held by another live session in the same home is rejected with
  `-32010 alias_taken`.
- **Bound to the connection.** When the session disconnects the alias
  is released; another session may then claim it. Queued DMs
  addressed to the alias survive the gap and are delivered to whoever
  next claims it (within retention).
- **Optional.** A session is always addressable by its `sid` whether
  or not it has an alias.

The session ID (`sid`) and alias both occupy the post-`#`
discriminator slot in the address grammar. To avoid ambiguity,
aliases must match `^[a-z][a-z0-9_-]{0,63}$` and must not start with
the reserved sid prefix `sess_`.

### Address grammar

| Form                       | Meaning                                              |
|----------------------------|------------------------------------------------------|
| `home:~/path`              | Broadcast — fans out to every session at that path   |
| `home:~/path#sess_abc`     | Direct — single session by sid                       |
| `home:~/path#architect`    | Direct — single session by alias (scoped to that home)|
| `topic:my-topic`           | Topic — fans out to every subscriber                 |

Topic names are free-form UTF-8 (no leading slash, no `#`, ≤ 256 bytes).
The peer namespace is per-user; cross-user collaboration is out of
scope for v0.1.

## 3. Bootstrap

`blemees-peerd` listens on a Unix domain socket owned by the user, mode `0600`.
That permission is the entire trust boundary in v0.1: any process
running as the same user may connect; processes belonging to other
users may not. There is no cryptographic handshake — it would only
gate against same-user attackers, which the socket permission does
not (and cannot) defend against anyway.

A peer client (typically the MCP sidecar — see §9) self-determines
its identity at startup, with no help from `blemees-peerd`:

- **Socket path** — by convention, `$XDG_RUNTIME_DIR/blemees/peerd.sock`,
  falling back to `/tmp/blemees-peerd-<uid>.sock`. Both `blemees-peerd` and any
  client resolve in the same order. Tests and unusual setups override
  via the `--socket` CLI flag.
- **home** — if the env var `BLEMEES_AGENT_HOME` is set, use its value
  verbatim; assumed already normalized per §2. Otherwise fall back to
  `os.path.realpath(os.getcwd())` and normalize.
- **sid** — if the env var `BLEMEES_AGENT_SID` is set and well-formed
  (matches `sess_` + 16 url-safe base64 chars), use it. Otherwise
  mint a fresh sid via `secrets.token_urlsafe(12)`.
- **alias** *(optional)* — if the env var `BLEMEES_AGENT_ALIAS` is
  set and well-formed (matches the alias grammar in §2), the sidecar
  calls `peer.set_alias` once after `peer.hello` to claim it. A
  malformed value is logged and ignored; an `alias_taken` collision
  is logged but non-fatal — the session is still addressable by
  `sid`, and the agent can retry via the `peer_set_alias` tool when
  the conflicting session disconnects.

`BLEMEES_AGENT_HOME`, `BLEMEES_AGENT_SID`, and `BLEMEES_AGENT_ALIAS`
are **generic agent-context conventions** exported by harnesses like
`blemees-agentd`. They are *not* peer-specific variables, and `blemees-peerd`
itself does not define them. The peer client reads them
opportunistically because:

- Some MCP hosts spawn stdio subprocesses with a cwd unrelated to the
  agent's actual working directory; an explicit env var survives
  that quirk where `os.getcwd()` would not.
- A harness with its own notion of "this agent's session ID" can
  flow that ID transitively into the MCP subprocess via env
  inheritance, so peer messages and the harness's logs share a
  correlatable identity.
- A harness that knows the agent's role (architect, tester,
  reviewer) can pre-claim the alias so peers can address the new
  session by role from the moment it joins, without waiting for
  the agent itself to figure out and claim its identity.

The client connects, sends `peer.hello {sid, home}`, and (if an
alias was provided) `peer.set_alias`. `blemees-peerd` registers the
connection under `(home, sid)` and the peer is live. Identity claims
are taken at face value — see §10 for the cases this *doesn't*
cover and would need real auth for.

## 4. Wire format

- Unix domain socket at `$XDG_RUNTIME_DIR/blemees/peerd.sock`, mode `0600`
- Newline-delimited JSON-RPC 2.0
- One connection per agent session; multiplexed methods + notifications

## 5. Methods (client → server)

### `peer.hello`

```json
{"sid": "sess_abc", "home": "~/foo"}
```
Returns `{"peer_id": "home:~/foo", "sid": "sess_abc", "server_version": "0.1"}`.
Must be the first method on a new connection.

`home` must already be normalized per §2 — `blemees-peerd` validates the form
(rejects un-normalized paths with `-32602 invalid_params`) but does
not re-canonicalize. `sid` must be unique across currently-connected
peers; reconnecting under an existing live `sid` returns
`-32011 sid_in_use`.

### `peer.set_alias`

```json
{"alias": "architect"}    // empty string clears the alias
```
Returns `{"ok": true, "alias": "architect"}`, or fails with
`-32010 alias_taken` if another live session in the same home already
holds the alias. A successful call replaces any previously set alias
on this session and emits a `peer.alias_changed` notification.

### `peer.set_dm_filter`

```json
{"patterns": ["home:~/projects/*", "home:~/foo#architect", "*#tester"]}
```
Returns `{"ok": true, "patterns": [...]}`. Restricts which DMs `blemees-peerd`
forwards to this session — a `peer.message` is delivered only if its
`from` address matches at least one pattern. Patterns use `fnmatch`
glob syntax over the canonical address string. Default on a new
connection is `["*"]` (accept all). The empty list `[]` blocks all
DMs; topic subscriptions are unaffected (those are always opt-in and
explicit).

### `peer.send`

```json
{"to": "home:~/bar" | "home:~/bar#sess_xyz" | "home:~/bar#architect",
 "body": <any-json>,
 "reply_to": "<message_id>"  // optional
}
```
Returns `{"message_id": "msg_...", "delivered": <int>}` — `delivered`
is the count of currently-connected recipients. Undelivered DMs are
queued under the addressed string; topic messages are not queued for
absent subscribers. If `to` uses an alias and no session currently
holds it, the message is queued under the alias address and delivered
when a session next claims it (within retention).

### `peer.publish`

```json
{"topic": "build-events", "body": <any-json>}
```
Returns `{"message_id": "msg_...", "subscribers": <int>}`.

### `peer.subscribe`

```json
{"topic": "build-events", "replay": 50}   // replay defaults to 0
```
Returns `{"topic": "build-events", "ok": true}`. Replay re-emits the
last N persisted messages on the topic as `peer.message` notifications
before the subscribe completes.

### `peer.unsubscribe`

```json
{"topic": "build-events"}
```

### `peer.list_peers`

Returns:
```json
{"peers": [
  {"peer_id": "home:~/foo",
   "sessions": [
     {"sid": "sess_abc", "alias": "architect", "subscriptions": ["build-events"]},
     {"sid": "sess_def", "alias": null,        "subscriptions": []}
   ]}
]}
```

### `peer.list_topics`

Returns `{"topics": [{"name": "...", "subscribers": <int>, "last_message_at": <ts>}]}`.

### `peer.history`

```json
{"address": "home:~/foo" | "topic:build-events",
 "since": "<message_id>",        // optional
 "limit": 100}
```
Returns `{"messages": [...]}` from persisted log.

## 6. Notifications (server → client)

### `peer.message`

```json
{"message_id": "msg_...",
 "from":       "home:~/foo#sess_abc",
 "from_alias": "architect",                 // null if sender has no alias
 "to":         "home:~/bar" | "topic:build-events",
 "body":       <any-json>,
 "reply_to":   "msg_...",                    // if any
 "ts":         1717000000.123}
```

`from` is always the canonical sid form so a recipient can correlate
a series of messages to a specific process even if the sender's alias
changes mid-conversation. `from_alias` is a convenience snapshot at
send time.

### `peer.peer_joined` / `peer.peer_left`

```json
{"peer_id": "home:~/foo", "sid": "sess_abc", "alias": "architect", "ts": ...}
```

### `peer.alias_changed`

```json
{"peer_id": "home:~/foo", "sid": "sess_abc",
 "alias":   "architect",   // null when cleared
 "previous": null,
 "ts": ...}
```
Emitted when any session sets, changes, or clears its alias.

### `peer.subscription_lost`

Emitted if `blemees-peerd` evicts a subscription (e.g. server shutdown grace).

## 7. Delivery & persistence

| Class  | Queue                                  | Persistence                                    |
|--------|----------------------------------------|------------------------------------------------|
| DM     | Per-recipient bounded (default 100, drop-oldest) | `$XDG_STATE_HOME/blemees/peerd/dms/<hash>.jsonl` |
| Topic  | No queue (pub-sub semantics)           | Per-topic ring buffer, default last 1000 msgs in `topics/<hash>.jsonl` |

- Hash is `sha256(address)[:16]` so on-disk filenames don't leak paths
  to anyone reading `ls`.
- Logs rotate weekly, keep 4 weeks, then deleted. Configurable.
- Cold start: `blemees-peerd` rehydrates ring buffers and queue tails into
  memory at boot.

## 8. Failure modes

| Situation                            | Behavior                                    |
|--------------------------------------|---------------------------------------------|
| `blemees-peerd` not running                  | Agent connect-refused; peer tools surface as unavailable, agent continues |
| `peer.hello` with un-normalized home | Returns `-32602 invalid_params`, connection closed |
| `peer.hello` with sid already live   | Returns `-32011 sid_in_use`, connection closed |
| `set_alias` collision                | Returns `-32010 alias_taken`; caller's existing alias (if any) is unchanged |
| Send to unknown DM recipient         | Returns `delivered: 0`; queued under that address; replayed on first hello matching it (within retention) |
| Send to unclaimed alias              | Returns `delivered: 0`; queued under `home:<path>#<alias>`; delivered to whoever next claims that alias in that home |
| Publish to topic with no subscribers | Returns `subscribers: 0`; message still persisted to ring buffer |
| Connection lost mid-session          | Queued DMs replay on reconnect; topic subscriptions drop and must be re-issued |

## 9. Agent integration

`blemees-agentd` is not involved. A peer client is anything that connects
to the socket and speaks `peer/1`; two flavors are expected:

1. **MCP sidecar (recommended).** A standalone Python entry point
   `blemees-peer-mcp` ships in this package. The user adds it to
   their agent's MCP config — wherever they configure other MCP
   servers (e.g. `~/.config/claude/mcp.json`,
   `~/.codex/config.toml`) — and it becomes available to every
   session of that agent. The sidecar exposes:
   - `peer_send(to, body)` → `peer.send`
   - `peer_publish(topic, body)` → `peer.publish`
   - `peer_subscribe(topic)` — long-lived subscription
   - `peer_inbox(limit)` — drains buffered `peer.message` notifications
   - `peer_await(timeout)` — blocks via streaming progress
     notifications until a message arrives, returns it as the call
     result
   - `peer_set_alias(alias)` → `peer.set_alias`
   - `peer_set_dm_filter(patterns)` → `peer.set_dm_filter`
   - resources: `peer://inbox` (subscribable), `peer://peers`, `peer://topics`

   The sidecar is an **stdio MCP server**: each MCP host spawns it
   as a subprocess of the agent, communicates over stdin/stdout, and
   the subprocess inherits the agent's environment — including
   `BLEMEES_AGENT_HOME`, `BLEMEES_AGENT_SID`, and (optionally)
   `BLEMEES_AGENT_ALIAS` if a harness like `blemees-agentd` set them (§3).
   The sidecar uses those env vars as its identity if present,
   otherwise falls back to `os.getcwd()` + `mint_sid()`, and
   connects to `blemees-peerd` at the conventional socket path.

2. **Direct `peer/1` client.** Anything that speaks newline-delimited
   JSON-RPC 2.0 over the Unix socket. Reference clients for Python
   and shell live in `examples/`.

The sidecar works regardless of how the agent process was spawned —
manually from a shell, by `blemees-agentd`, by a CI runner, or any other
harness. Nothing in `blemees-agentd` references `blemees-peerd` or this package.

## 10. Out of scope for v0.1

- **Authentication beyond Unix socket permission.** A same-user
  process can claim any `(sid, home)` it likes; `blemees-peerd` will believe
  it. This is acceptable because such a process can already read every
  file the user owns, including this one's transcripts. Cross-user,
  cross-host, or cross-trust-boundary scenarios all require real auth
  (signed tokens, mTLS, etc.) and are out of scope until federation.
- Federation across hosts (per-user, single-machine only)
- End-to-end encryption between peers — `blemees-peerd` is a trusted broker
- Per-host coordination of `sid` collision risk — sids are minted
  randomly with 96 bits of entropy, so practical collisions are
  nil; `blemees-peerd` rejects duplicates at hello but does not gracefully
  retry on the client side
- Backpressure beyond bounded queues / ring buffers
- Replay across `blemees-peerd` restarts spanning a process crash mid-write
  (durability is best-effort fsync-on-close-of-batch)
