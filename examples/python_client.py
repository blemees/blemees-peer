#!/usr/bin/env python3
"""Reference Python client for the blemees-peer/1 protocol.

Demonstrates connecting, sending a DM, subscribing to a topic, and
receiving notifications. Run two copies in two terminals to see them
talk to each other.

Usage:
    # Terminal 1 — start the daemon somewhere first
    blemees-peerd

    # Terminal 2
    python examples/python_client.py architect

    # Terminal 3
    python examples/python_client.py tester
"""

from __future__ import annotations

import argparse
import asyncio
import os

from blemees_peer.addressing import mint_sid, normalize_path
from blemees_peer.client import PeerClient
from blemees_peer.persistence import default_socket_path


async def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("alias", help="alias to claim (e.g. 'architect')")
    parser.add_argument("--socket", default=str(default_socket_path()))
    parser.add_argument("--home", default=normalize_path(os.getcwd()))
    args = parser.parse_args()

    client = PeerClient(args.socket)
    await client.connect()
    sid = mint_sid()
    hello = await client.hello(sid, args.home)
    print(f"connected: {hello}")
    await client.set_alias(args.alias)
    print(f"claimed alias: {args.alias}")
    await client.subscribe("demo")
    print("subscribed to topic 'demo'")
    print("type messages to publish to topic 'demo'; ^C to quit\n")

    async def reader() -> None:
        while True:
            note = await client.next_notification()
            method = note.get("method")
            params = note.get("params") or {}
            if method == "peer.message":
                origin = params.get("from_alias") or params.get("from")
                print(f"\n[{params['to']}] {origin}: {params['body']}\n> ", end="", flush=True)
            else:
                print(f"\n<{method}> {params}\n> ", end="", flush=True)

    reader_task = asyncio.create_task(reader())

    try:
        loop = asyncio.get_running_loop()
        while True:
            print("> ", end="", flush=True)
            line = await loop.run_in_executor(None, input)
            line = line.strip()
            if not line:
                continue
            await client.publish("demo", line)
    except (KeyboardInterrupt, EOFError):
        pass
    finally:
        reader_task.cancel()
        await client.close()
    return 0


if __name__ == "__main__":
    asyncio.run(main())
