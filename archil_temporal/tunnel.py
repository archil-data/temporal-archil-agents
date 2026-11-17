"""A local TCP tunnel to a Temporal server running in an Archil sandbox.

Only for testing with a self-hosted Temporal server (scripts/e2e.py runs one
in a sandbox). A sandbox's exposed port is served over TLS, but its TLS
handshake doesn't negotiate HTTP/2, which gRPC clients require. So clients
connect in plaintext to a local port, and this forwards each connection over
TLS. Temporal Cloud doesn't need any of this.
"""

from __future__ import annotations

import asyncio
import contextlib
import ssl


async def _pipe(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    try:
        while data := await reader.read(65536):
            writer.write(data)
            await writer.drain()
    finally:
        with contextlib.suppress(Exception):
            writer.close()


async def open_tunnel(host: str) -> str:
    """Start forwarding a local port to ``host``:443 and return its address."""
    tls = ssl.create_default_context()

    async def handle(client_reader, client_writer):
        try:
            reader, writer = await asyncio.open_connection(host, 443, ssl=tls, server_hostname=host)
        except OSError:
            client_writer.close()
            return
        await asyncio.gather(_pipe(client_reader, writer), _pipe(reader, client_writer), return_exceptions=True)

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    asyncio.get_running_loop().create_task(server.serve_forever())
    return f"127.0.0.1:{port}"
