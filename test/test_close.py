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
# A call must not return while a connection it closed is still closing; see _settle().
# These tests watch every protocol aiohttp makes and check, the moment a call returns, that
# each one is either closed or still open for reuse, never half-way.
#
import asyncio
import contextlib
import socket
import ssl
import threading
import time
from collections.abc import Iterator

import aiohttp
import pytest
from aiohttp import web
from aiohttp.client_proto import ResponseHandler
from aiohttp.test_utils import TestServer

from pelican_webdav_client import Client, NotFoundError

BOUND = 0.1
"""Stands in for CLOSE_TIMEOUT, so a test of a stalled peer does not wait a full second."""


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


@pytest.fixture(name="short_bound")
def fixture_short_bound(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("pelican_webdav_client.client.CLOSE_TIMEOUT", BOUND)


def half_closed(protocols: list[ResponseHandler]) -> list[ResponseHandler]:
    """The protocols aiohttp has asked to close that have not finished closing."""
    return [p for p in protocols if p.transport is None and p.closed is not None and not p.closed.done()]


def half_closed_after(protocols, url, call="stat", expect=None, **client_options):
    """The half-closed protocols at the moment a call on /foo/bar/ ends."""

    async def run() -> list[ResponseHandler]:
        async with Client(url, **client_options) as client:
            with expect or contextlib.nullcontext():
                await getattr(client, call)("/foo/bar/")
            return half_closed(protocols)

    unfinished = asyncio.run(run())
    assert protocols, "no connection was made, so this observed nothing"
    return unfinished


def closing_server(server, listing, status=207):
    """Answer PROPFIND /foo/bar/ and close the connection after the reply."""
    server.clear()
    server.expect_request("/foo/bar/", method="PROPFIND").respond_with_data(listing, status=status, content_type="application/xml", headers={"Connection": "close"})


@pytest.mark.parametrize("call", ["stat", "list", "exists", "is_collection"])
def test_a_call_returns_with_the_connection_closed_over_tls(tls_httpserver, httpclient_ssl_context, protocols, top_listing_response, call):
    closing_server(tls_httpserver, top_listing_response)
    assert half_closed_after(protocols, tls_httpserver.url_for("/"), call, ssl=httpclient_ssl_context) == []


def test_an_error_status_returns_with_the_connection_closed(tls_httpserver, httpclient_ssl_context, protocols, top_listing_response):
    closing_server(tls_httpserver, top_listing_response, status=404)
    assert half_closed_after(protocols, tls_httpserver.url_for("/"), expect=pytest.raises(NotFoundError), ssl=httpclient_ssl_context) == []


def test_a_call_returns_with_the_connection_closed_over_http(httpserver, protocols, top_listing_response):
    closing_server(httpserver, top_listing_response)
    assert half_closed_after(protocols, httpserver.url_for("/")) == []


def test_a_kept_connection_goes_back_to_the_pool(protocols, top_listing_response):
    """An aiohttp server keeps HTTP/1.1 connections alive, so the calls share one and nothing waits."""

    async def propfind(_request: web.Request) -> web.Response:
        return web.Response(status=207, text=top_listing_response, content_type="application/xml")

    async def run() -> list[bool]:
        app = web.Application()
        app.router.add_route("PROPFIND", "/foo/bar/", propfind)
        async with TestServer(app) as server, Client(str(server.make_url("/"))) as client:
            await client.stat("/foo/bar/")
            await client.stat("/foo/bar/")
            return [p.is_connected() for p in protocols]

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
    # A test that fails before connecting must not leave the thread blocked in accept().
    listener.settimeout(5)
    release = threading.Event()

    def serve() -> None:
        try:
            raw, _ = listener.accept()
        except TimeoutError:
            return
        try:
            with context.wrap_socket(raw, server_side=True) as conn:
                request = b""
                while b"\r\n\r\n" not in request:
                    chunk = conn.recv(65536)
                    if not chunk:
                        return  # The client went before sending a whole request.
                    request += chunk
                conn.sendall(response)
                release.wait(30)
        except OSError:
            return  # A handshake or connection the client abandoned.

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    try:
        yield listener.getsockname()[1], release
    finally:
        release.set()
        thread.join(5)
        listener.close()


async def all_closed(protocols: list[ResponseHandler]) -> None:
    """Wait for every connection to finish closing, so the loop can close clean."""
    # closed is None for a connection lost before anyone asked; that one is done already.
    futures = {closed for p in protocols if (closed := p.closed) is not None}
    if futures:
        await asyncio.wait(futures, timeout=5)
    for future in futures:
        if future.done() and not future.cancelled():
            future.exception()


async def timed_stat(port, ssl_context, protocols, release, **client_options) -> tuple[float, list[ResponseHandler]]:
    """How long a stat takes and what it left half-closed; then the peer goes, so the loop can close clean."""
    async with Client(f"https://127.0.0.1:{port}/", ssl=ssl_context, **client_options) as client:
        start = time.monotonic()
        await client.stat("/foo/bar/")
        elapsed = time.monotonic() - start
        unfinished = half_closed(protocols)
        release.set()
        await all_closed(protocols)
        return elapsed, unfinished


def small_reply(listing: str, *, close: bool = True) -> bytes:
    """A reply that arrives whole with its headers, so aiohttp releases its connection before the client sees it."""
    body = listing.encode()
    connection = "Connection: close\r\n" if close else ""
    return f"HTTP/1.1 207 Multi-Status\r\nContent-Type: application/xml\r\nContent-Length: {len(body)}\r\n{connection}\r\n".encode() + body


STALLED_BODY = b"HTTP/1.1 207 Multi-Status\r\nContent-Type: application/xml\r\nContent-Length: 1000000\r\n\r\n" + b"<" * 1000
"""A reply whose body never finishes, so its connection is still held when a call ends."""


def assert_waited_the_bound(elapsed: float) -> None:
    assert BOUND <= elapsed < BOUND + 2, f"the call took {elapsed:.2f}s"


@pytest.mark.usefixtures("short_bound")
def test_a_stalled_shutdown_is_aborted_after_the_bound(ca, httpclient_ssl_context, protocols):
    """
    exists() never reads the body, so the connection is still held when the call ends and
    the client can reach the transport: once the bound passes, it aborts it.
    """
    with stalled_tls_server(ca, STALLED_BODY) as (port, _):

        async def run() -> tuple[float, list[ResponseHandler]]:
            async with Client(f"https://127.0.0.1:{port}/", ssl=httpclient_ssl_context) as client:
                start = time.monotonic()
                assert await client.exists("/foo/bar/") is True
                return time.monotonic() - start, half_closed(protocols)

        elapsed, unfinished = asyncio.run(run())

    assert unfinished == [], "the stalled connection was not aborted"
    assert_waited_the_bound(elapsed)


@pytest.mark.usefixtures("short_bound")
def test_a_stalled_shutdown_is_aborted_without_a_transport(ca, httpclient_ssl_context, protocols, top_listing_response):
    """
    A small reply arrives whole with its headers and aiohttp closes the connection before
    the client sees the response, leaving the client no transport to abort. The session the
    client creates bounds the TLS shutdown itself, so asyncio aborts it within the call.
    """
    with stalled_tls_server(ca, small_reply(top_listing_response)) as (port, release):
        elapsed, unfinished = asyncio.run(timed_stat(port, httpclient_ssl_context, protocols, release))
    assert unfinished == [], "the stalled connection was not aborted"
    assert_waited_the_bound(elapsed)


@pytest.mark.usefixtures("short_bound")
def test_a_call_waits_for_a_redirect_hop_to_close(tls_httpserver, ca, httpclient_ssl_context, protocols, top_listing_response):
    """aiohttp closes the connection of a redirect itself, and only the final response reaches the client."""
    closing_server(tls_httpserver, top_listing_response)
    redirect = f"HTTP/1.1 307 Temporary Redirect\r\nLocation: {tls_httpserver.url_for('/foo/bar/')}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".encode()
    with stalled_tls_server(ca, redirect) as (port, release):
        elapsed, unfinished = asyncio.run(timed_stat(port, httpclient_ssl_context, protocols, release))
    assert unfinished == [], "the redirect's stalled connection was not aborted"
    assert_waited_the_bound(elapsed)


def cancelled_call(port, ssl_context, protocols, call) -> tuple[float, list[ResponseHandler]]:
    """Start a call on /foo/bar/, cancel it partway, and time how long the cancellation takes."""

    async def run() -> tuple[float, list[ResponseHandler]]:
        async with Client(f"https://127.0.0.1:{port}/", ssl=ssl_context) as client:
            task = asyncio.create_task(getattr(client, call)("/foo/bar/"))
            await asyncio.sleep(0.3)
            start = time.monotonic()
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            # No grace period: the cancellation itself waits for its aborts to land.
            return time.monotonic() - start, half_closed(protocols)

    return asyncio.run(run())


def test_a_call_cancelled_while_waiting_aborts_at_once(ca, httpclient_ssl_context, protocols):
    """exists() returns into the wait at once; cancelling it there aborts rather than strands the connection."""
    with stalled_tls_server(ca, STALLED_BODY) as (port, _):
        elapsed, unfinished = cancelled_call(port, httpclient_ssl_context, protocols, "exists")
    assert unfinished == [], "the cancelled call left its connection half-closed"
    assert elapsed < 0.2, f"cancelling took {elapsed:.2f}s"


def test_a_call_already_cancelling_does_not_wait(ca, httpclient_ssl_context, protocols):
    """list() is stuck reading the body when cancelled, so it reaches the wait already cancelling."""
    with stalled_tls_server(ca, STALLED_BODY) as (port, _):
        elapsed, unfinished = cancelled_call(port, httpclient_ssl_context, protocols, "list")
    assert unfinished == [], "the cancelled call left its connection half-closed"
    assert elapsed < 0.2, f"cancelling took {elapsed:.2f}s"


@pytest.mark.parametrize("own_session", [True, False], ids=["owned session", "passed session"])
def test_a_redirect_hop_is_settled_when_the_final_hop_fails(ca, httpclient_ssl_context, protocols, own_session):
    """
    The hop's connection is still closing when the request it led to fails, before any
    final response exists for aiohttp to hang the hop's history on. The client follows
    redirects itself, so it holds every hop's connection whichever session is in use, and
    a failed call waits for one that is closing. The hop here closes late, after the final
    hop has already failed, so a call that did not wait would return with it half-closed.
    """
    with stalled_tls_server(ca, b"") as (target, hang_up):
        hang_up.set()  # The target drops the connection as soon as the request is in.
        redirect = f"HTTP/1.1 307 Temporary Redirect\r\nLocation: https://127.0.0.1:{target}/foo/bar/\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
        with stalled_tls_server(ca, redirect.encode()) as (port, release):
            threading.Timer(0.3, release.set).start()

            async def run() -> list[ResponseHandler]:
                async with aiohttp.ClientSession() as session, Client(f"https://127.0.0.1:{port}/", ssl=httpclient_ssl_context, session=None if own_session else session) as client:
                    with pytest.raises(aiohttp.ClientError):
                        await client.stat("/foo/bar/")
                    unfinished = half_closed(protocols)
                    await all_closed(protocols)
                    return unfinished

            unfinished = asyncio.run(run())

    assert protocols, "no connection was made, so this observed nothing"
    assert protocols[0] not in unfinished, "the redirect hop was left half-closed"


def test_a_failed_call_does_not_wait_out_the_bound(ca, httpclient_ssl_context, protocols):
    """A call whose own timeout fires has spent its budget: it aborts at once rather than waiting CLOSE_TIMEOUT."""
    with stalled_tls_server(ca, STALLED_BODY) as (port, _):

        async def run() -> tuple[float, list[ResponseHandler]]:
            async with Client(f"https://127.0.0.1:{port}/", ssl=httpclient_ssl_context, timeout=aiohttp.ClientTimeout(total=0.3)) as client:
                start = time.monotonic()
                with pytest.raises((TimeoutError, aiohttp.ClientError)):
                    await client.list("/foo/bar/")
                return time.monotonic() - start, half_closed(protocols)

        elapsed, unfinished = asyncio.run(run())

    assert unfinished == [], "the timed-out call left its connection half-closed"
    assert elapsed < 0.3 + 0.3, f"a 0.3s timeout returned after {elapsed:.2f}s"


def test_a_swallowed_cancellation_does_not_stop_later_calls_waiting(tls_httpserver, httpclient_ssl_context, protocols, top_listing_response):
    """
    A task that caught a CancelledError without uncancel() still reports cancelling(). A
    later call on that task completes normally and must still wait, not abort.
    """
    closing_server(tls_httpserver, top_listing_response)

    async def run() -> list[ResponseHandler]:
        async with Client(tls_httpserver.url_for("/"), ssl=httpclient_ssl_context) as client:
            task = asyncio.current_task()
            assert task is not None
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await asyncio.sleep(1)
            assert task.cancelling(), "the setup did not leave the task cancelling"
            await client.stat("/foo/bar/")
            return half_closed(protocols)

    assert asyncio.run(run()) == []
    assert protocols, "no connection was made, so this observed nothing"


def test_a_passed_session_does_not_wait_twice_for_nothing(ca, httpclient_ssl_context, protocols, top_listing_response, monkeypatch):
    """
    In a session passed in, a connection aiohttp already dropped can neither be aborted by
    the client nor is bounded by it, so after the first wait there is nothing to wait for.
    """
    monkeypatch.setattr("pelican_webdav_client.client.CLOSE_TIMEOUT", 0.5)
    with stalled_tls_server(ca, small_reply(top_listing_response)) as (port, release):

        async def run() -> float:
            async with aiohttp.ClientSession() as session:
                elapsed, _ = await timed_stat(port, httpclient_ssl_context, protocols, release, session=session)
                return elapsed

        elapsed = asyncio.run(run())

    assert 0.5 <= elapsed < 0.75, f"the call took {elapsed:.2f}s"


def test_close_does_not_wait_for_a_stalled_pooled_connection(ca, httpclient_ssl_context, protocols, top_listing_response):
    """
    A kept-alive connection to a peer that never answers close_notify is pooled when the
    client closes. Nothing will reuse it, so close aborts it rather than waiting the bound.
    """
    with stalled_tls_server(ca, small_reply(top_listing_response, close=False)) as (port, _):

        async def run() -> tuple[float, list[ResponseHandler]]:
            client = Client(f"https://127.0.0.1:{port}/", ssl=httpclient_ssl_context)
            await client.stat("/foo/bar/")
            assert [p.is_connected() for p in protocols] == [True], "the connection should have been pooled"
            start = time.monotonic()
            await client.close()
            return time.monotonic() - start, half_closed(protocols)

        elapsed, unfinished = asyncio.run(run())

    assert unfinished == [], "close left the pooled connection half-closed"
    assert elapsed < 0.5, f"close took {elapsed:.2f}s"


def test_the_wait_is_skipped_if_aiohttp_loses_the_closed_future(tls_httpserver, httpclient_ssl_context, top_listing_response, monkeypatch):
    """The wait rests on an attribute aiohttp added recently; without it a call still works, as before the wait existed."""
    monkeypatch.delattr(ResponseHandler, "closed")
    closing_server(tls_httpserver, top_listing_response)

    async def run() -> bool:
        async with Client(tls_httpserver.url_for("/"), ssl=httpclient_ssl_context) as client:
            return (await client.stat("/foo/bar/")).is_collection

    assert asyncio.run(run()) is True
