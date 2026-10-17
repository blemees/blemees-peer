#!/usr/bin/env bash
# Tiny shell client for blemees-peer/1 — useful for poking blemees-peerd from the
# command line. Requires `socat` and `jq`.
#
# Usage:
#   examples/shell_client.sh hello      # connect + hello + read forever
#   examples/shell_client.sh send TO BODY
#   examples/shell_client.sh publish TOPIC BODY
#
# Writes each request as one JSON-RPC line; reads responses/notifications
# and pretty-prints them.

set -euo pipefail

# Same order as default_socket_path() in src/blemees_peer/persistence.py.
if [ -n "${XDG_RUNTIME_DIR:-}" ] && [ -d "$XDG_RUNTIME_DIR" ]; then
  DEFAULT_SOCKET="$XDG_RUNTIME_DIR/blemees/peerd.sock"
else
  DEFAULT_SOCKET="/tmp/blemees-peerd-$(id -u).sock"
fi
SOCKET="${PEERD_SOCKET:-$DEFAULT_SOCKET}"

# Same rules as normalize_path() in src/blemees_peer/addressing.py: resolve
# symlinks, then rewrite a path at or under the home directory with "~".
normalize_cwd() {
  real="$(pwd -P)"
  user_home="$(cd ~ 2>/dev/null && pwd -P)" || user_home="${HOME:-}"
  case "$real" in
    "$user_home") printf '~\n' ;;
    "$user_home"/*) printf '~/%s\n' "${real#"$user_home"/}" ;;
    *) printf '%s\n' "$real" ;;
  esac
}
HOME_PATH="${BLEMEES_AGENT_HOME:-$(normalize_cwd)}"
SID="${BLEMEES_AGENT_SID:-sess_$(python3 -c 'import secrets; print(secrets.token_urlsafe(12))')}"

cmd="${1:-hello}"
shift || true

case "$cmd" in
  hello)
    {
      jq -nc --arg sid "$SID" --arg home "$HOME_PATH" \
        '{jsonrpc:"2.0", id:1, method:"peer.hello", params:{sid:$sid, home:$home}}'
      sleep infinity
    } | socat - "UNIX-CONNECT:$SOCKET" | jq -c .
    ;;
  send)
    to="$1"; body="${2:-hi}"
    {
      jq -nc --arg sid "$SID" --arg home "$HOME_PATH" \
        '{jsonrpc:"2.0", id:1, method:"peer.hello", params:{sid:$sid, home:$home}}'
      jq -nc --arg to "$to" --arg body "$body" \
        '{jsonrpc:"2.0", id:2, method:"peer.send", params:{to:$to, body:$body}}'
      sleep 1
    } | socat - "UNIX-CONNECT:$SOCKET" | jq -c .
    ;;
  publish)
    topic="$1"; body="${2:-hi}"
    {
      jq -nc --arg sid "$SID" --arg home "$HOME_PATH" \
        '{jsonrpc:"2.0", id:1, method:"peer.hello", params:{sid:$sid, home:$home}}'
      jq -nc --arg topic "$topic" --arg body "$body" \
        '{jsonrpc:"2.0", id:2, method:"peer.publish", params:{topic:$topic, body:$body}}'
      sleep 1
    } | socat - "UNIX-CONNECT:$SOCKET" | jq -c .
    ;;
  *)
    echo "usage: $0 {hello|send TO BODY|publish TOPIC BODY}" >&2
    exit 2
    ;;
esac
