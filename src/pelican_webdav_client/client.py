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
from types import SimpleNamespace
from typing import TYPE_CHECKING, Literal
from urllib.parse import quote, unquote

import aiohttp
import yarl

from .exceptions import InvalidResponseError, NotFoundError, ResponseError
from .multistatus import MultistatusError, Resource, parse_multistatus, same_path

if TYPE_CHECKING:
    import ssl
    from collections.abc import AsyncIterator, Iterable, Mapping
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

    A redirect is followed, and the reply is read against the URL that finally answered,
    not the one asked for: a server that redirects one form of a resource's URL to another
    still describes the resource that was asked about.

    `token`, if given, is sent as a bearer token on every request. `ssl` is handed to
    aiohttp unchanged, so it takes True to verify certificates as usual, False to skip
    verification, or an ssl.SSLContext of your own, for example one that trusts a private
    certificate authority. `timeout` likewise.

    `session` is the aiohttp session to send requests through. Without one the client
    creates its own, on first use, and closes it in close(); a session passed in is left to
    its owner. The client is an async context manager that closes itself on exit.

    Every method reads or releases the response it gets before it returns or raises, so a
    client never holds a connection between calls. Once a response has arrived, a connection
    that is not kept for reuse has also finished closing by then, or has been given up to
    twice CLOSE_TIMEOUT to do so. Transport failures are aiohttp's own exceptions; an error
    status from the server is a ResponseError.
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
        TLS shutdowns bounded by CLOSE_TIMEOUT and redirect hops recorded; see _connector()
        and _record_hop().
        """
        if self._session is None:
            hops = aiohttp.TraceConfig()
            hops.on_request_redirect.append(_record_hop)
            self._session = aiohttp.ClientSession(connector=_connector(), trace_configs=[hops])
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
        waits for every connection the request used to settle; see _settle().
        """
        headers = {"Depth": str(depth)}
        if self._token:
            headers["Authorization"] = f"Bearer {self._token}"
        timeout = self.session.timeout if self._timeout is None else self._timeout

        logger.debug("%s %s (Depth: %s)", METHOD, url, depth)
        # Redirect hops, recorded as they happen in a session the client created, so that a
        # request that fails after a redirect still settles them; see _record_hop().
        hops: list[aiohttp.ClientResponse] = []
        connections: dict[ResponseHandler, asyncio.Transport | None] = {}
        outcome: _Outcome = "failed"
        try:
            async with self.session.request(METHOD, url, headers=headers, ssl=self._ssl, timeout=timeout, trace_request_ctx=hops if self._owns_session else None) as response:
                connections = _connections((*response.history, response))
                logger.debug("%s %s -> %s %s", METHOD, url, response.status, response.reason)
                if response.status >= 400:
                    body = (await response.read()).decode("utf-8", errors="replace")
                    raise ResponseError.for_status(METHOD, str(url), response.status, response.reason or "", body)
                yield response
            outcome = "settled"
        except ResponseError:
            outcome = "settled"  # The server answered; the connection closes as usual.
            raise
        except asyncio.CancelledError:
            outcome = "cancelled"
            raise
        except aiohttp.ClientResponseError as err:
            hops.extend(err.history)  # TooManyRedirects carries every hop, in any session.
            raise
        finally:
            await _settle({**_connections(hops), **connections}, outcome, bounded=self._owns_session)


_Outcome = Literal["settled", "failed", "cancelled"]
"""How a request ended: with a response from the server, with an error, or cancelled."""


def _connections(responses: Iterable[aiohttp.ClientResponse]) -> dict[ResponseHandler, asyncio.Transport | None]:
    """
    The protocols that carried `responses`, each with the transport it has now.

    aiohttp may have released a connection already, so response.connection does not do.
    _protocol is private: if it ever goes, the wait is skipped rather than failing.
    """
    return {p: p.transport for r in responses if (p := getattr(r, "_protocol", None)) is not None}


async def _record_hop(_session: aiohttp.ClientSession, context: SimpleNamespace, params: aiohttp.TraceRequestRedirectParams) -> None:
    """Add a redirect's response to the list the request passed as its trace_request_ctx."""
    hops = context.trace_request_ctx
    if isinstance(hops, list):
        hops.append(params.response)


async def _settle(connections: dict[ResponseHandler, asyncio.Transport | None], outcome: _Outcome, *, bounded: bool) -> None:
    """
    Wait until released connections are back in the pool or fully closed.

    aiohttp closes a connection it will not reuse by asking its transport to close, which
    for TLS starts a shutdown that ends some loop iterations later, or never, if the peer
    does not answer. A caller that closes its event loop before then, as fsspec's sync
    wrappers and asyncio.run() do, strands the transport.

    `connections` maps each protocol to the transport it had when its response arrived; a
    protocol whose transport aiohttp had already dropped maps to None and cannot be aborted
    here. `bounded` says the session is one the client created, whose TLS shutdowns asyncio
    aborts itself after CLOSE_TIMEOUT (see _connector()), so waiting for those ends too.

    After a response ("settled"), a connection gets CLOSE_TIMEOUT to close. One still
    closing is then aborted, and the call waits up to CLOSE_TIMEOUT again for whatever can
    still finish: what it aborted, and, if bounded, the rest. A call is held up by at most
    twice CLOSE_TIMEOUT. After a failure, the connection is broken and the caller's own
    timeout may be spent, so it is aborted at once and only the wait for what can still
    finish remains, at most CLOSE_TIMEOUT. A cancelled call aborts and waits only for its
    aborts to land, a few loop iterations; a second cancellation then finds them done.

    A request that fails before its final response arrives (a timeout or reset while
    awaiting the headers) leaves that last connection out of reach: aiohttp gives no handle
    on it, and it finishes closing within the session's TLS shutdown timeout.
    """
    closing = {
        closed: transport
        for protocol, transport in connections.items()
        # Reading `closed` creates the future, so check the pooled case first.
        if protocol.transport is None and (closed := protocol.closed) is not None
    }
    if not closing:
        return
    # gather also retrieves a lost connection's error, which asyncio would otherwise log.
    settled = asyncio.gather(*closing, return_exceptions=True)
    if outcome == "settled":
        try:
            await asyncio.wait({settled}, timeout=CLOSE_TIMEOUT)
        except asyncio.CancelledError:
            _abort(closing)
            raise
        if settled.done():
            return
        logger.debug("%d connection(s) did not close within %ss; aborting", sum(not f.done() for f in closing), CLOSE_TIMEOUT)
    aborted = _abort(closing)
    pending = {future for future in closing if not future.done()}
    finishing = pending if bounded and outcome != "cancelled" else pending & aborted
    if finishing:
        await asyncio.wait(finishing, timeout=CLOSE_TIMEOUT)


def _abort(closing: Mapping[asyncio.Future[None], asyncio.Transport | None]) -> set[asyncio.Future[None]]:
    """Abort every transport still closing, without waiting; return the futures of those aborted."""
    aborted = set()
    for future, transport in closing.items():
        if transport is not None and not future.done():
            transport.abort()
            aborted.add(future)
    return aborted


def _connector() -> aiohttp.TCPConnector:
    """
    A connector for a session the client creates, whose TLS shutdowns abort after CLOSE_TIMEOUT.

    When a reply arrives whole with its headers, or on a redirect hop, aiohttp closes a
    connection it will not reuse before the client sees the response, so _settle() has no
    transport to abort. With this, asyncio aborts it instead, after the same bound rather
    than its default thirty seconds.

    The public ssl_shutdown_timeout parameter warns of its removal in aiohttp 4.0, and
    silencing that needs warnings.catch_warnings(), which changes process-wide state and is
    not thread safe. The connector's own attribute, read at each connection and present
    since 3.12.6, sets the same thing without a warning; if a release drops it, the session
    falls back to aiohttp's default.
    """
    connector = aiohttp.TCPConnector()
    if hasattr(connector, "_ssl_shutdown_timeout"):
        connector._ssl_shutdown_timeout = CLOSE_TIMEOUT
    return connector
