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

SOCKET="${PEERD_SOCKET:-${XDG_RUNTIME_DIR:-/tmp}/blemees/peerd.sock}"
HOME_PATH="${BLEMEES_AGENT_HOME:-$(python3 -c 'import os; print(os.path.realpath(os.getcwd()))')}"
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
