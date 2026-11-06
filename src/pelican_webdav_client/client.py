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

# A WebDAV client that speaks PROPFIND and nothing else. pelicanfs needs PROPFIND to list
# collections and to tell objects from collections, and nothing more of WebDAV.

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal
from urllib.parse import quote, unquote

import aiohttp
import yarl

from .exceptions import InvalidResponseError, NotFoundError, ResponseError
from .multistatus import MultistatusError, Resource, parse_multistatus, same_path

if TYPE_CHECKING:
    import ssl
    from collections.abc import AsyncIterator
    from types import TracebackType
    from typing import Self

    from aiohttp.client_proto import ResponseHandler

logger = logging.getLogger(__name__)

METHOD = "PROPFIND"

Depth = Literal[0, 1, "infinity"]
"""The Depth header of a PROPFIND: the resource alone, with its children, or its whole subtree."""

CLOSE_TIMEOUT = 1.0
"""
Seconds a call waits for a closing connection before it aborts it, and then again for the
abort to land: a call is held up by at most twice this. See _settle().
"""

REDIRECTS = frozenset({301, 302, 307, 308})
"""The statuses a request follows with the same PROPFIND; see Client._follow()."""

MAX_REDIRECTS = 10
"""How many redirects a request follows before giving up, as aiohttp does."""


@dataclass(frozen=True, slots=True)
class _Reply:
    """Where and how the server answered: the final URL's path, after any redirect, and the status."""

    path: str
    status: int
    reason: str


class Client:
    """
    Ask a WebDAV server about its resources with PROPFIND.

    `base_url` is the server, with any path prefix the resources live under; the paths
    given to the methods are appended to it, and the paths reported back are relative to
    it. A client rooted at a Pelican origin's data endpoint therefore asks and answers in
    namespace paths, with the endpoint prefix hidden. Paths are taken percent-decoded,
    exactly as the server would see them, and encoded here: a "%" in a path is a literal
    percent sign.

    A redirect is followed, as aiohttp would follow it, and the reply is read against the
    URL that finally answered, not the one asked for: a server that redirects one form of
    a resource's URL to another still describes the resource that was asked about.

    `token`, if given, is sent as a bearer token on every request. `ssl` is handed to
    aiohttp unchanged, so it takes True to verify certificates as usual, False to skip
    verification, or an ssl.SSLContext of your own, for example one that trusts a private
    certificate authority. `timeout` likewise.

    `session` is the aiohttp session to send requests through. Without one the client
    creates its own, on first use, and closes it in close(); a session passed in is left to
    its owner. The client is an async context manager that closes itself on exit.

    Every method reads or releases the responses it gets before it returns or raises, so a
    client never holds a connection between calls. A connection that is not kept for reuse
    is also closed by then, not merely closing, or has been given up to twice CLOSE_TIMEOUT
    to close, with two exceptions. A request that fails before any response arrives leaves
    a connection aiohttp gives no handle on; in a session the client created it finishes
    closing on its own within CLOSE_TIMEOUT, but the call does not wait for it. And in a
    session passed in, a connection aiohttp closed before the client saw its response is
    waited for but can be neither aborted nor bounded, so a peer that never completes the
    TLS shutdown leaves it closing. Transport failures are aiohttp's own exceptions; an
    error status from the server is a ResponseError.
    """

    def __init__(
        self,
        base_url: str,
        *,
        token: str | None = None,
        ssl: bool | ssl.SSLContext = True,
        session: aiohttp.ClientSession | None = None,
        timeout: aiohttp.ClientTimeout | None = None,
    ) -> None:
        self._base_url = yarl.URL(base_url)
        if not self._base_url.scheme or not self._base_url.host:
            msg = f"base_url must be an absolute URL with a scheme and a host, not {base_url!r}"
            raise ValueError(msg)
        self._base_path = self._base_url.raw_path.rstrip("/")
        self._base_path_decoded = unquote(self._base_path)
        self._token = token
        self._ssl = ssl
        self._timeout = timeout
        self._session = session
        self._owns_session = session is None

    @property
    def base_url(self) -> str:
        """The server this client talks to, as given."""
        return str(self._base_url)

    @property
    def session(self) -> aiohttp.ClientSession:
        """
        The session requests go through. Created on first use if none was passed in, with
        TLS shutdowns bounded by CLOSE_TIMEOUT; see _connector().
        """
        if self._session is None:
            self._session = aiohttp.ClientSession(connector=_connector())
        return self._session

    def url(self, path: str) -> yarl.URL:
        """
        The URL of the resource at the decoded path `path`.

        The path is percent-encoded here and the URL marked as encoded, so nothing further
        down re-encodes it: "a b" becomes "a%20b" and "100%" becomes "100%25". A leading
        slash is supplied if missing. A trailing slash is kept, and matters to a server:
        Pelican origins answer a collection's slash form and reject an object's.
        """
        if not path.startswith("/"):
            path = f"/{path}"
        return self._base_url.with_path(self._base_path + quote(path, safe="/"), encoded=True)

    async def propfind(self, path: str, *, depth: Depth = 0) -> list[Resource]:
        """
        Every resource the server describes in answer to a PROPFIND of `path`.

        With depth 0 that is the resource itself; with depth 1, the resource and its
        children; with "infinity", its whole subtree, if the server allows it. The
        request carries no body, which asks for all properties.

        Raises ResponseError (or a subclass) on an error status, and InvalidResponseError
        if a successful reply is not a multistatus document.
        """
        _reply, resources = await self._propfind(self.url(path), depth)
        return self._relative(resources)

    async def stat(self, path: str) -> Resource:
        """
        What the server says about the resource at `path`.

        Raises NotFoundError if the server answers 404, or answers without describing
        `path`.
        """
        url = self.url(path)
        reply, resources = await self._propfind(url, 0)
        for resource in resources:
            if same_path(resource.path, reply.path):
                return resource.relative_to(self._base_path_decoded)
        raise NotFoundError(METHOD, str(url), reply.status, f"{reply.reason}: the reply does not describe {reply.path}")

    async def list(self, path: str) -> list[Resource]:
        """
        The resources in the collection at `path`, without the collection itself.

        The request goes to the collection's slash form whether or not `path` ends with
        one; see url().
        """
        if not path.endswith("/"):
            path = f"{path}/"
        reply, resources = await self._propfind(self.url(path), 1)
        return self._relative([resource for resource in resources if not same_path(resource.path, reply.path)])

    async def is_collection(self, path: str) -> bool:
        """
        Whether the resource at `path` is a collection. Raises as stat() does.
        """
        return (await self.stat(path)).is_collection

    async def exists(self, path: str) -> bool:
        """
        Whether the server has a resource at `path`, judged by the status alone.

        A 404 is False and any success is True, whatever the body says; stat() would
        also need the reply to describe `path`, which not every server does for every
        resource. Any other error status raises.
        """
        try:
            async with self._response(self.url(path), 0):
                pass
        except NotFoundError:
            return False
        return True

    async def close(self) -> None:
        """
        Close the session, if this client created it.
        """
        if self._owns_session and self._session is not None:
            # Nothing will reuse the pooled connections now, so have aiohttp abort their TLS
            # shutdowns at close rather than wait out the bound, as its own default does.
            _set_ssl_shutdown_timeout(self._session.connector, 0)
            await self._session.close()
            self._session = None

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, exc_type: type[BaseException] | None, exc: BaseException | None, traceback: TracebackType | None) -> None:
        await self.close()

    async def _propfind(self, url: yarl.URL, depth: Depth) -> tuple[_Reply, list[Resource]]:
        """
        Send a PROPFIND and parse the multistatus it returns.

        The resources come back with their paths as the server named them, so that they can
        be compared with the path the server answered at; relative_to() then strips the
        base path for the caller. The reply records that final URL, after any redirect.
        """
        async with self._response(url, depth) as response:
            reply = _Reply(response.url.path, response.status, response.reason or "")
            body = await response.read()
        try:
            resources = parse_multistatus(body, reply.path)
        except MultistatusError as err:
            raise InvalidResponseError(METHOD, str(url), str(err)) from err
        return reply, resources

    def _relative(self, resources: list[Resource]) -> list[Resource]:
        return [resource.relative_to(self._base_path_decoded) for resource in resources]

    @asynccontextmanager
    async def _response(self, url: yarl.URL, depth: Depth) -> AsyncIterator[aiohttp.ClientResponse]:
        """
        Send a PROPFIND and yield its response, raising for an error status.

        The response is released when the block ends, so a caller that does not read the
        body still gives the connection back. On an error status the body is read for the
        error's message, which also leaves the connection clean. Either way, the block
        waits for the response's connection to settle; see _settle().
        """
        headers = {"Depth": str(depth)}
        if self._token:
            headers["Authorization"] = f"Bearer {self._token}"
        timeout = self.session.timeout if self._timeout is None else self._timeout

        # Every hop's connection, with the transport it had when its response arrived.
        connections: dict[ResponseHandler, asyncio.Transport | None] = {}
        outcome: _Outcome = "failed"
        try:
            async with await self._follow(url, headers, timeout, connections) as response:
                if response.status >= 400:
                    body = (await response.read()).decode("utf-8", errors="replace")
                    outcome = "settled"  # The server answered; the connection closes as usual.
                    raise ResponseError.for_status(METHOD, str(url), response.status, response.reason or "", body)
                yield response
            outcome = "settled"
        except asyncio.CancelledError:
            outcome = "cancelled"
            raise
        finally:
            # Settled together, so a call is still held up by at most twice CLOSE_TIMEOUT.
            if connections:
                await asyncio.gather(*(_settle(protocol, transport, outcome, bounded=self._owns_session) for protocol, transport in connections.items()))

    async def _follow(self, url: yarl.URL, headers: dict[str, str], timeout: aiohttp.ClientTimeout, connections: dict[ResponseHandler, asyncio.Transport | None]) -> aiohttp.ClientResponse:
        """
        Send a PROPFIND to `url`, follow its redirects, and return the response that ends them.

        Redirects are followed here rather than by aiohttp so that every hop's response,
        and with it the connection that carried it, goes through `connections`, whatever
        session is in use; aiohttp shows a caller only the hops of a request that got a
        final response. The rules are aiohttp's: a 301, 302, 307 or 308 with a Location is
        followed with the same method, to an http or https URL resolved against the one
        that redirected, and a redirect to another origin drops the Authorization header.
        A 303 asks for a GET of another resource, which a PROPFIND cannot do, and is
        returned as it is.
        """
        headers = dict(headers)
        history: list[aiohttp.ClientResponse] = []
        while True:
            logger.debug("%s %s (Depth: %s)", METHOD, url, headers["Depth"])
            response = await self.session.request(METHOD, url, headers=headers, ssl=self._ssl, timeout=timeout, allow_redirects=False)
            # aiohttp may have released the connection already, so response.connection does
            # not do. _protocol is private: if it ever goes, the wait is skipped, not failed.
            protocol = getattr(response, "_protocol", None)
            if protocol is not None:
                connections.setdefault(protocol, protocol.transport)
            logger.debug("%s %s -> %s %s", METHOD, url, response.status, response.reason)
            location = response.headers.get("Location") if response.status in REDIRECTS else None
            if location is None:
                return response
            response.release()
            history.append(response)
            if len(history) >= MAX_REDIRECTS:
                raise aiohttp.TooManyRedirects(response.request_info, tuple(history))
            try:
                target = url.join(yarl.URL(location))
                if target.scheme not in {"http", "https"}:
                    raise aiohttp.NonHttpUrlRedirectClientError(location)
                if target.origin() != url.origin():
                    headers.pop("Authorization", None)
            except ValueError as err:
                raise aiohttp.InvalidUrlRedirectClientError(location, str(err)) from err
            url = target


_Outcome = Literal["settled", "failed", "cancelled"]
"""How a request ended: with a response from the server, with an error, or cancelled."""


async def _settle(protocol: ResponseHandler, transport: asyncio.Transport | None, outcome: _Outcome, *, bounded: bool) -> None:
    """
    Wait until a released connection is back in the pool or fully closed.

    aiohttp closes a connection it will not reuse by asking its transport to close, which
    for TLS starts a shutdown that ends some loop iterations later, or never, if the peer
    does not answer. A caller that closes its event loop before then, as fsspec's sync
    wrappers and asyncio.run() do, strands the transport.

    `transport` is the one the protocol had when its response arrived, or None if aiohttp
    had already dropped it, in which case it cannot be aborted here. `bounded` says the
    session is one the client created, whose TLS shutdowns asyncio aborts itself after
    CLOSE_TIMEOUT (see _connector()), so waiting for one ends either way.

    After a response ("settled"), the connection gets CLOSE_TIMEOUT to close. If it is
    still closing, it is aborted, and the call waits up to CLOSE_TIMEOUT again if it can
    still finish: if it was aborted, or the session is bounded. A call is held up by at
    most twice CLOSE_TIMEOUT. After a failure, the caller's own timeout may be spent, so
    the connection is aborted at once if it can be, and a single wait of up to
    CLOSE_TIMEOUT remains. A cancelled call, whether cancelled before or during the wait,
    aborts what it can and waits only for that abort to land, a few loop iterations,
    before the cancellation goes on.

    A request that fails before its final response arrives (a timeout or reset while
    awaiting the headers) leaves that last connection out of reach: aiohttp gives no handle
    on it, and it finishes closing within the session's TLS shutdown timeout.
    """
    # Reading `closed` creates the future, so check the pooled case first. `closed` is
    # recent (aiohttp 3.13): if it is not there, the wait is skipped rather than failing.
    if protocol.transport is not None:
        return
    closed = getattr(protocol, "closed", None)
    if closed is None:
        return
    # A connection lost with an error sets it on the future, which asyncio logs if nobody
    # retrieves it; reading the future above created it, so retrieve it whenever it lands.
    closed.add_done_callback(_retrieve)
    cancelled: asyncio.CancelledError | None = None
    if outcome == "settled":
        try:
            await asyncio.wait({closed}, timeout=CLOSE_TIMEOUT)
        except asyncio.CancelledError as err:
            cancelled, outcome = err, "cancelled"
        else:
            if closed.done():
                return
            logger.debug("connection did not close within %ss; aborting", CLOSE_TIMEOUT)
    aborted = False
    if transport is not None and not closed.done():
        transport.abort()
        aborted = True
    # After a failure nothing has been waited for yet, and in a bounded session asyncio
    # finishes the shutdown itself; otherwise only an abort can still end it.
    if not closed.done() and (aborted or outcome == "failed" or (outcome == "settled" and bounded)):
        await asyncio.wait({closed}, timeout=CLOSE_TIMEOUT)
    if cancelled is not None:
        raise cancelled


def _retrieve(future: asyncio.Future[None]) -> None:
    if not future.cancelled():
        future.exception()


def _connector() -> aiohttp.TCPConnector:
    """
    A connector for a session the client creates, whose TLS shutdowns end within CLOSE_TIMEOUT.

    When a reply arrives whole with its headers, or on a redirect hop, aiohttp closes a
    connection it will not reuse before the client sees the response, so _settle() has no
    transport to abort. With this, asyncio aborts it instead, after the same bound rather
    than its default thirty seconds.
    """
    connector = aiohttp.TCPConnector()
    _set_ssl_shutdown_timeout(connector, CLOSE_TIMEOUT)
    return connector


def _set_ssl_shutdown_timeout(connector: aiohttp.BaseConnector | None, seconds: float) -> None:
    """
    Set the TLS shutdown bound of `connector`, if it has one.

    The public ssl_shutdown_timeout parameter warns of its removal in aiohttp 4.0, and
    silencing that needs warnings.catch_warnings(), which changes process-wide state and is
    not thread safe. The connector's own attribute, read at each connection and present
    throughout the supported releases, sets the same thing without a warning; if a release
    drops it, the session keeps aiohttp's default.
    """
    if connector is not None and hasattr(connector, "_ssl_shutdown_timeout"):
        connector._ssl_shutdown_timeout = seconds
