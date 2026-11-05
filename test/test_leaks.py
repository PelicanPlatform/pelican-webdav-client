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

import asyncio

#
# The client must never hold a connection between calls. These tests are the ones pelicanfs
# needed to guard its workaround for aiowebdav2, which left responses unreleased; here they
# guard the client itself, which reads or releases every response before it returns.
#
import gc
from contextlib import contextmanager

import aiohttp.connector

from pelican_webdav_client import Client


@contextmanager
def connections_finalised_for(server):
    """
    Watch the connections aiohttp finalises for "server" while the block runs.

    Yields (finalised, leaked): every connection aiohttp let go of, and the subset that
    still held a protocol at that moment - which is the state aiohttp itself reports as
    "Unclosed connection". Hooking Connection.__del__ is the only way to observe the leak
    from outside, and the count is only complete once a collection has run, so the block
    collects on the way in and on the way out.

    Connection.__del__, _key and _protocol are aiohttp internals, so callers assert that
    "finalised" is non-empty as well as that "leaked" is empty: if any of those move, the
    hook stops recording and an assertion on "leaked" alone would pass without having
    observed anything at all.
    """
    finalised, leaked = [], []
    original_del = aiohttp.connector.Connection.__del__

    def counting_del(self, *args, **kwargs):
        key = self._key
        if (key.host, key.port) == (server.host, server.port):
            finalised.append(key)
            if self._protocol is not None:
                leaked.append(key)
        original_del(self, *args, **kwargs)

    aiohttp.connector.Connection.__del__ = counting_del
    gc.collect()
    try:
        yield finalised, leaked
    finally:
        gc.collect()
        aiohttp.connector.Connection.__del__ = original_del


def connections_held_by(client):
    """
    How many connections "client" currently has checked out of its pool.

    connector._acquired is an aiohttp internal, but it is the only view of the property
    that matters here: a response that has not been released keeps its connection checked
    out, so this stays at zero only if each call really does release as it ends.
    """
    return len(client.session.connector._acquired)


# aiohttp finishes a small body along with the headers and releases the connection by
# itself, which would hide a leak for reasons that have nothing to do with the code under
# test. Measured against aiohttp 3.12: under roughly 20 KB nothing ever looks leaked, and
# between 20 KB and 100 KB it is intermittent. Each entry below is about 190 bytes, so
# this is ~1.5 MB - far enough clear of that band to be reliable, and cheap to build.
_ENTRIES_TOO_LARGE_TO_ARRIVE_WITH_HEADERS = 8000


def large_multistatus_response():
    """A PROPFIND body too large for aiohttp to have finished reading with the headers."""
    entries = "".join(
        f"<d:response><d:href>/foo/bar/{i}</d:href><d:propstat><d:prop><d:getcontentlength>13</d:getcontentlength><d:resourcetype/></d:prop>"
        "<d:status>HTTP/1.1 200 OK</d:status></d:propstat></d:response>"
        for i in range(_ENTRIES_TOO_LARGE_TO_ARRIVE_WITH_HEADERS)
    )
    return f'<?xml version="1.0"?><d:multistatus xmlns:d="DAV:">{entries}</d:multistatus>'


def expect_propfind(server, status):
    server.expect_request("/foo/bar/", method="PROPFIND").respond_with_data(large_multistatus_response(), status=status, content_type="application/xml")


async def exists_through_client(server):
    async with Client(server.url_for("/")) as client:
        return await client.exists("/foo/bar/")


async def list_through_client(server):
    async with Client(server.url_for("/")) as client:
        return len(await client.list("/foo/bar/"))


def test_exists_releases_an_unread_body(httpserver):
    """exists() looks only at the status, so it has to give back a body it never reads."""
    expect_propfind(httpserver, status=207)

    with connections_finalised_for(httpserver) as (finalised, leaked):
        assert asyncio.run(exists_through_client(httpserver)) is True

    assert finalised, "no connection to the test server was finalised, so this observed nothing"
    assert leaked == [], "the response was not released, so its connection leaked"


def test_exists_releases_the_response_on_an_error_status(httpserver):
    """The object-does-not-exist path raises inside the client; the response must still be released."""
    expect_propfind(httpserver, status=404)

    with connections_finalised_for(httpserver) as (finalised, leaked):
        assert asyncio.run(exists_through_client(httpserver)) is False

    assert finalised, "no connection to the test server was finalised, so this observed nothing"
    assert leaked == [], "the response was not released, so its connection leaked"


def test_list_releases_the_response(httpserver):
    expect_propfind(httpserver, status=207)

    with connections_finalised_for(httpserver) as (finalised, leaked):
        assert asyncio.run(list_through_client(httpserver)) == _ENTRIES_TOO_LARGE_TO_ARRIVE_WITH_HEADERS

    assert finalised, "no connection to the test server was finalised, so this observed nothing"
    assert leaked == [], "the response was not released, so its connection leaked"


def test_client_does_not_hold_connections_between_calls(threaded_httpserver):
    """
    A reused client must not hold a connection once a call has returned.

    Nothing bounds how many calls a client serves or how long it is kept, so a connection
    held until the client closes is a leak deferred, not prevented. This needs the threaded
    server: held-open connections are exactly what it is looking for, and the
    single-threaded one stops serving while any are held.
    """
    threaded_httpserver.clear()
    expect_propfind(threaded_httpserver, status=207)

    async def held_after_each_call():
        held = []
        async with Client(threaded_httpserver.url_for("/")) as client:
            for _ in range(3):
                assert await client.exists("/foo/bar/") is True
                held.append(connections_held_by(client))
                assert len(await client.list("/foo/bar/")) == _ENTRIES_TOO_LARGE_TO_ARRIVE_WITH_HEADERS
                held.append(connections_held_by(client))
        return held

    assert asyncio.run(held_after_each_call()) == [0] * 6
