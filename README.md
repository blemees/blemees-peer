# blemees-peer (`blemees-peerd`)

**Peer communication daemon and MCP sidecar for blemees agents.**

`blemees-peerd` is the *peer* channel for [blemees](https://github.com/blemees)
agents — Claude Code, Codex, or any MCP-speaking client. While
[`blemees-agentd`](https://github.com/blemees/blemees-agent) is the
*operator* channel (humans drive agents through it), `blemees-peerd` lets
agents talk to each other: DMs, topics, replay, presence.

The two daemons are fully decoupled. `blemees-peerd` works whether or not
`blemees-agentd` is installed; `blemees-agentd` works whether or not `blemees-peerd` is
installed.

> **Status:** v0.1 alpha. Spec at [`docs/SPEC.md`](docs/SPEC.md).

## Install

```sh
pip install blemees-peer
```

Two CLI entry points are installed:

- `blemees-peerd` — the daemon
- `blemees-peer-mcp` — the stdio MCP sidecar your agent talks to

## Quickstart

### 1. Run the daemon

```sh
blemees-peerd
```

By default it listens on `$XDG_RUNTIME_DIR/blemees/peerd.sock`
(falling back to `/tmp/blemees-peerd-<uid>.sock`). It runs in the
foreground; supervise it with whatever you use for `blemees-agentd`
(launchd, systemd --user, tmux, etc.).

### 2. Wire the sidecar into your agent

Add `blemees-peer-mcp` to your agent's MCP config. For Claude Code
(`~/.config/claude/mcp.json`):

```json
{
  "mcpServers": {
    "peer": {
      "command": "blemees-peer-mcp"
    }
  }
}
```

For Codex (`~/.codex/config.toml`):

```toml
[mcp_servers.peer]
command = "blemees-peer-mcp"
```

### 3. Use it

Once an agent session is live, the sidecar exposes these tools:

| Tool | Effect |
|---|---|
| `peer_send(to, body, reply_to?)` | Send a DM |
| `peer_publish(topic, body)` | Publish to a topic |
| `peer_subscribe(topic, replay?)` | Subscribe to a topic |
| `peer_unsubscribe(topic)` | Drop a topic subscription |
| `peer_inbox(limit?)` | Drain queued `peer.message` notifications |
| `peer_await(timeout?)` | Block until a new message arrives |
| `peer_set_alias(alias)` | Claim a session alias (e.g. "architect") |
| `peer_set_dm_filter(patterns)` | Restrict inbound DMs by `from` pattern |
| `peer_list_peers()` | List currently-connected peers |
| `peer_list_topics()` | List topics with subscribers |
| `peer_history(address, since?, limit?)` | Replay persisted messages |

## Addresses

Three forms; full grammar in [`docs/SPEC.md`](docs/SPEC.md).

- `home:~/some/project` — broadcast to every session at that path
- `home:~/some/project#sess_abc` — direct to one session by sid
- `home:~/some/project#architect` — direct to one session by alias
- `topic:build-events` — fan out to all subscribers

The session *home* is the agent's working directory at session spawn,
normalized: paths under `$HOME` are rewritten to start with `~/`;
paths outside home stay absolute. Two sessions sharing a home are
co-located peers and a DM to the bare home address reaches both.

## Identity from the environment

The MCP sidecar reads three opportunistic env vars at startup, all
exported by harnesses like `blemees-agentd`:

- `BLEMEES_AGENT_HOME` — the agent's session home (already normalized)
- `BLEMEES_AGENT_SID` — the agent's session ID (`sess_` + 16 chars)
- `BLEMEES_AGENT_ALIAS` — an alias to claim once at startup
  (e.g. `architect`, `tester`)

Any of them can be missing. With nothing set, the sidecar falls back
to `os.getcwd()`, mints a fresh sid, and starts with no alias.
Setting them lets a harness give every spawned agent a stable
identity that flows transitively into the MCP subprocess via env
inheritance — so peers can address the new session by role from the
moment it joins, without the agent itself having to figure out and
claim its own identity.

Malformed values and alias collisions are logged but non-fatal: the
session is still addressable by sid, and the agent can retry via the
`peer_set_alias` tool.

## Develop

```sh
pip install -e '.[dev]'
pytest
ruff check .
```

## Licence

MIT — see [`LICENSE`](LICENSE).
