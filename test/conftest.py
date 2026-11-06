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
# Fixtures shared by the test suite.
#
# Tests talk to pytest-httpserver's `httpserver`, a plain http server on a free port, and
# drive the client with asyncio.run() so every test owns its event loop and closes it,
# which turns any session or connection left open into a ResourceWarning, and pytest is
# configured to fail on warnings.
#
# Two more servers exist for the tests that need them: `tls_httpserver` speaks https with
# a certificate from a throwaway authority, for the test that the `ssl` option reaches
# aiohttp, and `threaded_httpserver` serves connections in parallel, for the test that
# connections are not held open between calls.
#
import ssl
from pathlib import Path

import pytest
import trustme
from pytest_httpserver import HTTPServer

RESOURCES = Path(__file__).parent / "resources"


@pytest.fixture(scope="session", name="httpserver_listen_address")
def fixture_httpserver_listen_address():
    """Let the OS pick the port, so parallel runs and leftover sockets can't collide."""
    return ("127.0.0.1", 0)


def _server(ssl_context=None, threaded=False):
    server = HTTPServer(host="127.0.0.1", port=0, ssl_context=ssl_context, threaded=threaded)
    server.start()
    return server


@pytest.fixture(scope="session", name="ca")
def fixture_ca():
    """A throwaway certificate authority."""
    return trustme.CA()


@pytest.fixture(scope="session", name="httpclient_ssl_context")
def fixture_httpclient_ssl_context(ca):
    """A client-side context that trusts `ca` and nothing unusual beyond it."""
    with ca.cert_pem.tempfile() as ca_temp_path:
        return ssl.create_default_context(cafile=ca_temp_path)


@pytest.fixture(scope="session", name="server_ssl_context")
def fixture_server_ssl_context(ca):
    """A server-side context with a certificate for localhost and 127.0.0.1, signed by `ca`."""
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ca.issue_cert("localhost", "127.0.0.1").configure_cert(context)
    return context


@pytest.fixture(scope="session", name="tls_httpserver")
def fixture_tls_httpserver(server_ssl_context):
    """An https server with a certificate signed by `ca`. Call clear() before use."""
    server = _server(ssl_context=server_ssl_context)
    yield server
    server.clear()
    if server.is_running():
        server.stop()


@pytest.fixture(scope="session", name="threaded_httpserver")
def fixture_threaded_httpserver():
    """
    A server that serves connections in parallel rather than one at a time.

    The default server is single threaded, so a connection left open blocks the next
    request from being served at all. A test that checks connections are not being held
    open needs a second request to go through while the first would still be held.
    Call clear() before use.
    """
    server = _server(threaded=True)
    yield server
    server.clear()
    if server.is_running():
        server.stop()


@pytest.fixture
def top_listing_response():
    """Listing of /foo/bar/: itself, three objects, and the collections folder1 and folder2."""
    return (RESOURCES / "top_xml_response.xml").read_text()


@pytest.fixture
def file1_listing_response():
    """The single object /foo/bar/file1.txt."""
    return (RESOURCES / "file1_xml_response.xml").read_text()
