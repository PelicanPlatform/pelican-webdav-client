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

import aiohttp
import pytest

from pelican_webdav_client import Client


@pytest.mark.parametrize(
    ("path", "expected_path"),
    [
        ("/foo/a b", "/foo/a b"),
        ("/foo/100%", "/foo/100%"),
        ("/foo/a%20b", "/foo/a%20b"),
        ("/foo/ü/ñ.txt", "/foo/ü/ñ.txt"),
        ("/foo/a:b@c+d;e=f&g", "/foo/a:b@c+d;e=f&g"),
        ("/foo/q?x=1#frag", "/foo/q?x=1#frag"),
        ("foo/bar", "/foo/bar"),
        ("/", "/"),
    ],
)
def test_paths_are_taken_literally(httpserver, path, expected_path):
    """A path is percent-decoded input: whatever it contains reaches the server as that path."""
    httpserver.expect_request(expected_path, method="PROPFIND").respond_with_data("", status=200)

    async def main():
        async with Client(httpserver.url_for("/")) as client:
            return await client.exists(path)

    assert asyncio.run(main()) is True
    request = httpserver.log[-1][0]
    assert request.path == expected_path
    assert request.query_string == b""


def test_url():
    client = Client("https://origin.example.org:8443")
    assert str(client.url("/foo/a b/100%/")) == "https://origin.example.org:8443/foo/a%20b/100%25/"
    assert str(client.url("foo")) == "https://origin.example.org:8443/foo"
    assert str(client.url("/")) == "https://origin.example.org:8443/"
    assert client.url("/foo/a b").path == "/foo/a b"


@pytest.mark.parametrize("base", ["/base", "/base/"])
def test_base_url_with_a_path(httpserver, base):
    httpserver.expect_request("/base/foo/bar", method="PROPFIND").respond_with_data("", status=200)

    async def main():
        async with Client(httpserver.url_for(base)) as client:
            return await client.exists("/foo/bar")

    assert asyncio.run(main()) is True


@pytest.mark.parametrize("base_url", ["", "origin.example.org", "/just/a/path", "https://"])
def test_base_url_must_be_absolute(base_url):
    with pytest.raises(ValueError, match="absolute URL"):
        Client(base_url)


def test_token_is_sent_as_bearer(httpserver):
    httpserver.expect_request("/foo/bar", method="PROPFIND", headers={"Authorization": "Bearer secret"}).respond_with_data("", status=200)

    async def main():
        async with Client(httpserver.url_for("/"), token="secret") as client:
            return await client.exists("/foo/bar")

    assert asyncio.run(main()) is True


def test_no_token_means_no_authorization_header(httpserver):
    httpserver.expect_request("/foo/bar", method="PROPFIND").respond_with_data("", status=200)

    async def main():
        async with Client(httpserver.url_for("/")) as client:
            return await client.exists("/foo/bar")

    assert asyncio.run(main()) is True
    assert "Authorization" not in httpserver.log[-1][0].headers


def test_owned_session_is_closed_on_exit(httpserver):
    httpserver.expect_request("/foo/bar", method="PROPFIND").respond_with_data("", status=200)

    async def main():
        async with Client(httpserver.url_for("/")) as client:
            await client.exists("/foo/bar")
            session = client.session
            assert not session.closed
        return session.closed

    assert asyncio.run(main()) is True


def test_a_given_session_is_left_open(httpserver):
    httpserver.expect_request("/foo/bar", method="PROPFIND").respond_with_data("", status=200)

    async def main():
        async with aiohttp.ClientSession() as session:
            async with Client(httpserver.url_for("/"), session=session) as client:
                assert client.session is session
                await client.exists("/foo/bar")
            return session.closed

    assert asyncio.run(main()) is False


def test_timeout_is_forwarded(httpserver):
    """A timeout goes to aiohttp as given; here one too short to connect within."""
    httpserver.expect_request("/foo/bar", method="PROPFIND").respond_with_data("", status=200)

    async def main():
        async with Client(httpserver.url_for("/"), timeout=aiohttp.ClientTimeout(total=1e-9)) as client:
            return await client.exists("/foo/bar")

    with pytest.raises(TimeoutError):
        asyncio.run(main())


def test_ssl_context_reaches_aiohttp(tls_httpserver, httpclient_ssl_context):
    """An SSLContext given as `ssl` is what verifies the server, so a private CA can be trusted."""
    tls_httpserver.clear()
    tls_httpserver.expect_request("/foo/bar", method="PROPFIND").respond_with_data("", status=200)

    async def main():
        async with Client(tls_httpserver.url_for("/"), ssl=httpclient_ssl_context) as client:
            return await client.exists("/foo/bar")

    assert asyncio.run(main()) is True


def test_ssl_defaults_to_verifying(tls_httpserver):
    tls_httpserver.clear()
    tls_httpserver.expect_request("/foo/bar", method="PROPFIND").respond_with_data("", status=200)

    async def main():
        async with Client(tls_httpserver.url_for("/")) as client:
            return await client.exists("/foo/bar")

    with pytest.raises(aiohttp.ClientConnectorCertificateError):
        asyncio.run(main())


def test_ssl_false_skips_verification(tls_httpserver):
    tls_httpserver.clear()
    tls_httpserver.expect_request("/foo/bar", method="PROPFIND").respond_with_data("", status=200)

    async def main():
        async with Client(tls_httpserver.url_for("/"), ssl=False) as client:
            return await client.exists("/foo/bar")

    assert asyncio.run(main()) is True
