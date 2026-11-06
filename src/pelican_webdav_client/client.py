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
Seconds a call waits for a connection it closed to finish closing, before aborting it.

A TLS connection is not closed until both sides have exchanged close_notify; a peer that
never answers would otherwise keep the call waiting for asyncio's own shutdown timeout.
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
    client never holds a connection between calls. A connection that is not kept for reuse
    is also closed by then, not merely asked to close: a caller that ends its event loop
    straight after a call leaves no transport behind. Transport failures are aiohttp's own
    exceptions; an error status from the server is a ResponseError.
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
        The session requests go through. Created on first use if none was passed in.
        """
        if self._session is None:
            self._session = aiohttp.ClientSession()
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
        ends only once the connection is back in the pool or fully closed; see _settle().
        """
        headers = {"Depth": str(depth)}
        if self._token:
            headers["Authorization"] = f"Bearer {self._token}"
        timeout = self.session.timeout if self._timeout is None else self._timeout

        logger.debug("%s %s (Depth: %s)", METHOD, url, depth)
        protocol: ResponseHandler | None = None
        transport: asyncio.BaseTransport | None = None
        try:
            async with self.session.request(METHOD, url, headers=headers, ssl=self._ssl, timeout=timeout) as response:
                protocol = _protocol_of(response)
                transport = protocol.transport if protocol is not None else None
                logger.debug("%s %s -> %s %s", METHOD, url, response.status, response.reason)
                if response.status >= 400:
                    body = (await response.read()).decode("utf-8", errors="replace")
                    raise ResponseError.for_status(METHOD, str(url), response.status, response.reason or "", body)
                yield response
        finally:
            if protocol is not None:
                await _settle(protocol, transport)


def _protocol_of(response: aiohttp.ClientResponse) -> ResponseHandler | None:
    """
    The protocol that carried `response`.

    While the response holds its connection, that is public. But when the whole response
    arrives with its headers, aiohttp releases the connection before the caller sees the
    response, and the only handle left is the private _protocol attribute, present
    throughout aiohttp 3.
    """
    connection = response.connection
    if connection is not None:
        return connection.protocol
    return getattr(response, "_protocol", None)


async def _settle(protocol: ResponseHandler, transport: asyncio.BaseTransport | None) -> None:
    """
    Wait until a released connection is back in the pool or fully closed.

    aiohttp closes a connection it will not reuse by asking its transport to close, which
    for TLS starts a shutdown that finishes some loop iterations later, or never, if the
    peer does not answer. Waiting for it here means a caller that closes its event loop
    straight after a call does not strand a transport. The wait is bounded by
    CLOSE_TIMEOUT, after which the transport, if it was still reachable, is aborted.
    """
    if protocol.transport is not None:
        return  # Pooled for reuse, or still open: nothing is closing.
    closed = protocol.closed
    if closed is None:
        return  # Already closed, before anything asked to wait for it.
    if not closed.done():
        await asyncio.wait({closed}, timeout=CLOSE_TIMEOUT)
    if not closed.done() and transport is not None:
        logger.debug("connection did not close within %ss; aborting it", CLOSE_TIMEOUT)
        transport.abort()
        await asyncio.wait({closed}, timeout=CLOSE_TIMEOUT)
    # A connection lost with an error sets that as the future's exception. It is not this
    # call's concern, but asyncio logs an exception nobody retrieved, and reading the
    # future here created it, so retrieve it, now or whenever it lands.
    closed.add_done_callback(_retrieve)


def _retrieve(future: asyncio.Future[None]) -> None:
    if not future.cancelled():
        future.exception()
