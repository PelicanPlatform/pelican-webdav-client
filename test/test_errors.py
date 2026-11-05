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

import pytest

from pelican_webdav_client import (
    Client,
    ForbiddenError,
    MethodNotAllowedError,
    NotFoundError,
    ResponseError,
    UnauthorizedError,
    WebDavError,
)


@pytest.mark.parametrize(
    ("status", "error_class"),
    [
        (401, UnauthorizedError),
        (403, ForbiddenError),
        (404, NotFoundError),
        (405, MethodNotAllowedError),
        (409, ResponseError),
        (500, ResponseError),
        (507, ResponseError),
    ],
)
def test_error_status(httpserver, status, error_class):
    """Each error status raises the matching class, carrying the status and the body."""
    httpserver.expect_request("/foo/bar", method="PROPFIND").respond_with_data("the server's explanation", status=status, content_type="text/plain")

    async def main():
        async with Client(httpserver.url_for("/")) as client:
            return await client.stat("/foo/bar")

    with pytest.raises(error_class) as excinfo:
        asyncio.run(main())

    error = excinfo.value
    assert type(error) is error_class
    assert isinstance(error, ResponseError)
    assert isinstance(error, WebDavError)
    assert error.method == "PROPFIND"
    assert error.url == httpserver.url_for("/foo/bar")
    assert error.status == status
    assert error.body == "the server's explanation"
    assert str(status) in str(error)


def test_exists_raises_on_other_errors(httpserver):
    """Only a 404 means "no"; anything else is a failure the caller must see."""
    httpserver.expect_request("/foo/bar", method="PROPFIND").respond_with_data("", status=500)

    async def main():
        async with Client(httpserver.url_for("/")) as client:
            return await client.exists("/foo/bar")

    with pytest.raises(ResponseError) as excinfo:
        asyncio.run(main())
    assert excinfo.value.status == 500


def test_list_raises_on_404(httpserver):
    httpserver.expect_request("/foo/missing/", method="PROPFIND").respond_with_data("", status=404)

    async def main():
        async with Client(httpserver.url_for("/")) as client:
            return await client.list("/foo/missing")

    with pytest.raises(NotFoundError):
        asyncio.run(main())


def test_undecodable_error_body(httpserver):
    httpserver.expect_request("/foo/bar", method="PROPFIND").respond_with_data(b"\xff\xfe", status=500)

    async def main():
        async with Client(httpserver.url_for("/")) as client:
            return await client.stat("/foo/bar")

    with pytest.raises(ResponseError) as excinfo:
        asyncio.run(main())
    assert excinfo.value.body == "��"


def test_transport_errors_are_not_wrapped():
    """A server that cannot be reached is aiohttp's error, not one of this package's."""
    import aiohttp
    from pytest_httpserver import HTTPServer

    # A server of our own, stopped before the request, so its port is known to be closed
    server = HTTPServer(host="127.0.0.1", port=0)
    server.start()
    url = server.url_for("/")
    server.stop()

    async def main():
        async with Client(url) as client:
            return await client.exists("/foo/bar")

    with pytest.raises(aiohttp.ClientConnectionError):
        asyncio.run(main())
