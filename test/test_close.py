"""
Copyright (C) 2026, Pelican Project, Morgridge Institute for Research

Licensed under the Apache License, Version 2.0 (the "License"); you
may not use this file except in compliance with the License.  You may
obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

#
# A call must not return while a connection it closed is still closing. aiohttp closes a
# connection it will not reuse by asking the transport to close, and for TLS that starts a
# shutdown that ends some loop iterations later; a caller that closes its event loop in
# between, as fsspec's sync wrappers and asyncio.run() do, strands the transport. These
# tests watch every protocol aiohttp makes and check, the moment a call returns, that each
# one is either closed or still open for reuse, never half-way.
#
import asyncio
import contextlib
import socket
import ssl
import threading
import time
from collections.abc import Iterator

import pytest
from aiohttp import web
from aiohttp.client_proto import ResponseHandler

from pelican_webdav_client import Client, NotFoundError
from pelican_webdav_client.client import CLOSE_TIMEOUT

LISTING = (
    '<?xml version="1.0"?><d:multistatus xmlns:d="DAV:"><d:response><d:href>/foo/bar/</d:href>'
    "<d:propstat><d:prop><d:resourcetype><d:collection/></d:resourcetype></d:prop>"
    "<d:status>HTTP/1.1 200 OK</d:status></d:propstat></d:response></d:multistatus>"
)


@pytest.fixture(name="protocols")
def fixture_protocols(monkeypatch: pytest.MonkeyPatch) -> list[ResponseHandler]:
    """Every protocol aiohttp connects while the test runs."""
    made: list[ResponseHandler] = []
    original = ResponseHandler.connection_made

    def recording(self: ResponseHandler, transport: asyncio.BaseTransport) -> None:
        made.append(self)
        original(self, transport)

    monkeypatch.setattr(ResponseHandler, "connection_made", recording)
    return made


def half_closed(protocols: list[ResponseHandler]) -> list[ResponseHandler]:
    """The protocols aiohttp has asked to close that have not finished closing."""
    return [p for p in protocols if p.transport is None and not p._connection_lost_called]


def closing_server(server, status=207):
    """Answer PROPFIND /foo/bar/ and close the connection after the reply."""
    server.clear()
    server.expect_request("/foo/bar/", method="PROPFIND").respond_with_data(LISTING, status=status, content_type="application/xml", headers={"Connection": "close"})


@pytest.mark.parametrize("call", ["stat", "list", "exists", "is_collection"])
def test_a_call_returns_with_the_connection_closed_over_tls(tls_httpserver, httpclient_ssl_context, protocols, call):
    closing_server(tls_httpserver)

    async def run() -> list[ResponseHandler]:
        async with Client(tls_httpserver.url_for("/"), ssl=httpclient_ssl_context) as client:
            await getattr(client, call)("/foo/bar/")
            return half_closed(protocols)

    assert asyncio.run(run()) == []
    assert protocols, "no connection was made, so this observed nothing"


def test_an_error_status_returns_with_the_connection_closed(tls_httpserver, httpclient_ssl_context, protocols):
    closing_server(tls_httpserver, status=404)

    async def run() -> list[ResponseHandler]:
        async with Client(tls_httpserver.url_for("/"), ssl=httpclient_ssl_context) as client:
            with pytest.raises(NotFoundError):
                await client.stat("/foo/bar/")
            return half_closed(protocols)

    assert asyncio.run(run()) == []
    assert protocols, "no connection was made, so this observed nothing"


def test_a_call_returns_with_the_connection_closed_over_http(httpserver, protocols):
    closing_server(httpserver)

    async def run() -> list[ResponseHandler]:
        async with Client(httpserver.url_for("/")) as client:
            await client.stat("/foo/bar/")
            return half_closed(protocols)

    assert asyncio.run(run()) == []
    assert protocols, "no connection was made, so this observed nothing"


def test_a_kept_connection_goes_back_to_the_pool(protocols):
    """An aiohttp server keeps HTTP/1.1 connections alive, so the calls share one and nothing waits."""

    async def propfind(_request: web.Request) -> web.Response:
        return web.Response(status=207, text=LISTING, content_type="application/xml")

    async def run() -> list[bool]:
        app = web.Application()
        app.router.add_route("PROPFIND", "/foo/bar/", propfind)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        try:
            async with Client(f"http://127.0.0.1:{port}/") as client:
                await client.stat("/foo/bar/")
                await client.stat("/foo/bar/")
                return [p.is_connected() for p in protocols]
        finally:
            await runner.cleanup()

    assert asyncio.run(run()) == [True], "both calls should have used one pooled connection"


@contextlib.contextmanager
def stalled_tls_server(ca, response: bytes) -> Iterator[tuple[int, threading.Event]]:
    """
    A TLS server that sends `response` and then neither reads nor closes.

    It never answers the client's close_notify, so a TLS shutdown started by the client
    cannot finish until the client gives up on it. Set the event to let it close.
    """
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ca.issue_cert("127.0.0.1").configure_cert(context)
    listener = socket.create_server(("127.0.0.1", 0))
    release = threading.Event()

    def serve() -> None:
        raw, _ = listener.accept()
        with context.wrap_socket(raw, server_side=True) as conn:
            request = b""
            while b"\r\n\r\n" not in request:
                request += conn.recv(65536)
            conn.sendall(response)
            release.wait(30)

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    try:
        yield listener.getsockname()[1], release
    finally:
        release.set()
        thread.join(5)
        listener.close()


def test_a_stalled_shutdown_is_aborted_after_the_bound(ca, httpclient_ssl_context, protocols):
    """
    exists() never reads the body, so the connection is still held when the call ends and
    the client can reach the transport: once the bound passes, it aborts it.
    """
    head = b"HTTP/1.1 207 Multi-Status\r\nContent-Type: application/xml\r\nContent-Length: 1000000\r\n\r\n"
    with stalled_tls_server(ca, head + b"<" * 1000) as (port, _release):

        async def run() -> tuple[float, list[ResponseHandler]]:
            async with Client(f"https://127.0.0.1:{port}/", ssl=httpclient_ssl_context) as client:
                start = time.monotonic()
                assert await client.exists("/foo/bar/") is True
                return time.monotonic() - start, half_closed(protocols)

        elapsed, unfinished = asyncio.run(run())

    assert unfinished == [], "the stalled connection was not aborted"
    assert CLOSE_TIMEOUT <= elapsed < CLOSE_TIMEOUT + 2, f"the call took {elapsed:.2f}s"


def test_a_stalled_shutdown_does_not_hang_the_call(ca, httpclient_ssl_context, protocols):
    """
    A small reply arrives whole with its headers and aiohttp closes the connection before
    the client sees the response, leaving no transport to abort. The call still returns
    once the bound passes; the shutdown is left to finish when the peer closes.
    """
    body = LISTING.encode()
    head = f"HTTP/1.1 207 Multi-Status\r\nContent-Type: application/xml\r\nContent-Length: {len(body)}\r\nConnection: close\r\n\r\n".encode()
    with stalled_tls_server(ca, head + body) as (port, release):

        async def run() -> float:
            async with Client(f"https://127.0.0.1:{port}/", ssl=httpclient_ssl_context) as client:
                start = time.monotonic()
                await client.stat("/foo/bar/")
                elapsed = time.monotonic() - start
                release.set()
                # Let the shutdown finish now the peer has gone, so the loop closes clean.
                for protocol in protocols:
                    if protocol.closed is not None:
                        await asyncio.wait({protocol.closed}, timeout=5)
                return elapsed

        elapsed = asyncio.run(run())

    assert CLOSE_TIMEOUT <= elapsed < CLOSE_TIMEOUT + 2, f"the call took {elapsed:.2f}s"
