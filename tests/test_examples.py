"""Run examples/shell_client.sh against a real daemon."""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest

from blemees_peer.addressing import normalize_path
from blemees_peer.client import PeerClient
from blemees_peer.persistence import Persistence, default_socket_path
from blemees_peer.server import Server

SHELL_CLIENT = Path(__file__).resolve().parent.parent / "examples" / "shell_client.sh"

# Stands in for socat when it is not installed (GitHub's macOS runners lack
# it). It does what the script needs from `socat - UNIX-CONNECT:<path>`:
# copy stdin to the socket, then copy the socket to stdout until EOF.
_SOCAT_SHIM = """\
import socket, sys, threading
s = socket.socket(socket.AF_UNIX)
s.connect(sys.argv[2].split(":", 1)[1])
def pump():
    try:
        for line in sys.stdin.buffer:
            s.sendall(line)
        s.shutdown(socket.SHUT_WR)
    except OSError:
        pass
threading.Thread(target=pump, daemon=True).start()
while chunk := s.recv(65536):
    sys.stdout.buffer.write(chunk)
    sys.stdout.buffer.flush()
"""

# Records how the script calls socat, without connecting anywhere.
_SOCAT_RECORDER = """\
import sys
open(sys.argv[0] + ".args", "w").write("\\n".join(sys.argv[1:]))
sys.stdin.buffer.read()
"""

# Rejects "infinity" like BSD sleep on macOS, so the test fails on Linux too.
_BSD_SLEEP = """\
#!/bin/sh
case "$1" in
  ''|*[!0-9.]*) echo "usage: sleep seconds" >&2; exit 1 ;;
esac
exec /bin/sleep "$@"
"""


def _require(*tools: str) -> None:
    for tool in tools:
        if shutil.which(tool) is None:
            pytest.skip(f"examples/shell_client.sh needs {tool!r}, which is not on PATH")


def _fake_socat(bin_dir: Path, source: str) -> Path:
    bin_dir.mkdir(parents=True, exist_ok=True)
    path = bin_dir / "socat"
    path.write_text(f"#!{sys.executable}\n{source}")
    path.chmod(0o755)
    return path


def _script_env(home: Path, xdg: Path | None, bin_dir: Path | None = None) -> dict[str, str]:
    env = {
        k: v
        for k, v in os.environ.items()
        if k not in ("PEERD_SOCKET", "XDG_RUNTIME_DIR", "BLEMEES_AGENT_HOME", "BLEMEES_AGENT_SID")
    }
    env["HOME"] = str(home)
    if xdg is not None:
        env["XDG_RUNTIME_DIR"] = str(xdg)
    if bin_dir is not None:
        env["PATH"] = f"{bin_dir}{os.pathsep}{env['PATH']}"
    return env


@pytest.mark.parametrize("xdg_case", ["existing", "missing", "unset"])
def test_shell_client_socket_fallback_matches_daemon(xdg_case, tmp_path, monkeypatch) -> None:
    _require("bash", "jq", "python3")
    xdg = {"existing": tmp_path / "run", "missing": tmp_path / "gone", "unset": None}[xdg_case]
    if xdg_case == "existing":
        xdg.mkdir()
    socat = _fake_socat(tmp_path / "bin", _SOCAT_RECORDER)
    env = _script_env(tmp_path, xdg, socat.parent)

    monkeypatch.delenv("XDG_RUNTIME_DIR", raising=False)
    if xdg is not None:
        monkeypatch.setenv("XDG_RUNTIME_DIR", str(xdg))
    expected = default_socket_path()

    proc = subprocess.run(
        ["bash", str(SHELL_CLIENT), "publish", "t", "hi"],
        env=env,
        cwd=tmp_path,
        capture_output=True,
        timeout=30,
    )
    assert proc.returncode == 0, proc.stderr.decode()
    args = Path(str(socat) + ".args").read_text().splitlines()
    assert args == ["-", f"UNIX-CONNECT:{expected}"]


async def test_shell_client_hello_succeeds_under_home(tmp_path, monkeypatch) -> None:
    _require("bash", "jq", "python3")
    # A symlinked $HOME: normalize_path resolves it, so the script must too.
    real_home = tmp_path / "real-home"
    (real_home / "proj").mkdir(parents=True)
    home = tmp_path / "home"
    home.symlink_to(real_home)
    cwd = home / "proj"
    # Short dir: the socket path must fit in sun_path (104 bytes on macOS).
    xdg = Path(tempfile.mkdtemp(prefix="bp-", dir="/tmp"))
    bin_dir = None
    if shutil.which("socat") is None:
        bin_dir = _fake_socat(tmp_path / "bin", _SOCAT_SHIM).parent
    env = _script_env(home, xdg, bin_dir)

    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setenv("XDG_RUNTIME_DIR", str(xdg))
    expected_home = normalize_path(str(cwd))
    assert expected_home == "~/proj"

    server = Server(default_socket_path(), Persistence(tmp_path / "state"))
    await server.start()
    serve_task = asyncio.create_task(server.serve_forever())
    try:
        proc = await asyncio.create_subprocess_exec(
            "bash",
            str(SHELL_CLIENT),
            "publish",
            "shell-topic",
            "hi",
            env=env,
            cwd=cwd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        out, err = await asyncio.wait_for(proc.communicate(), timeout=30)
        assert proc.returncode == 0, err.decode()
        replies = {m.get("id"): m for m in map(json.loads, out.decode().splitlines())}
        assert replies[1].get("result", {}).get("peer_id") == f"home:{expected_home}", replies
        assert "result" in replies[2], replies
    finally:
        serve_task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await serve_task
        await server.shutdown()
        shutil.rmtree(xdg, ignore_errors=True)


async def test_shell_client_hello_stays_connected(tmp_path, monkeypatch) -> None:
    """`hello` must keep the connection open; `sleep infinity` failed on macOS."""
    _require("bash", "jq")
    xdg = Path(tempfile.mkdtemp(prefix="bp-", dir="/tmp"))
    bin_dir = tmp_path / "bin"
    if shutil.which("socat") is None:
        _fake_socat(bin_dir, _SOCAT_SHIM)
    bin_dir.mkdir(exist_ok=True)
    (bin_dir / "sleep").write_text(_BSD_SLEEP)
    (bin_dir / "sleep").chmod(0o755)
    env = _script_env(tmp_path, xdg, bin_dir)
    env["BLEMEES_AGENT_HOME"] = "/tmp/peer-test-shell-hello"
    env["BLEMEES_AGENT_SID"] = "sess_SHELLSHELLSHELL1"

    monkeypatch.setenv("XDG_RUNTIME_DIR", str(xdg))
    server = Server(default_socket_path(), Persistence(tmp_path / "state"))
    await server.start()
    serve_task = asyncio.create_task(server.serve_forever())
    observer = PeerClient(default_socket_path())

    async def connected() -> bool:
        info = await observer.list_peers()
        return any(p["peer_id"] == "home:/tmp/peer-test-shell-hello" for p in info["peers"])

    proc = await asyncio.create_subprocess_exec(
        "bash",
        str(SHELL_CLIENT),
        "hello",
        env=env,
        cwd=tmp_path,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
        start_new_session=True,
    )
    try:
        await observer.connect()
        await observer.hello("sess_OBSVOBSVOBSVOBS2", "/tmp/peer-test-shell-observer")
        for _ in range(100):
            if proc.returncode is not None or await connected():
                break
            await asyncio.sleep(0.1)
        await asyncio.sleep(1.0)
        assert proc.returncode is None, (await proc.stderr.read()).decode()
        assert await connected()
    finally:
        with contextlib.suppress(ProcessLookupError):
            os.killpg(proc.pid, signal.SIGTERM)
        await proc.wait()
        await observer.close()
        serve_task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await serve_task
        await server.shutdown()
        shutil.rmtree(xdg, ignore_errors=True)
