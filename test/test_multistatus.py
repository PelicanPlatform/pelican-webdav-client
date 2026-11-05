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

from datetime import UTC, datetime, timedelta, timezone

import pytest

from pelican_webdav_client import Resource
from pelican_webdav_client.multistatus import MultistatusError, parse_multistatus, same_path


def multistatus(*responses):
    return ('<?xml version="1.0" encoding="utf-8"?><D:multistatus xmlns:D="DAV:">' + "".join(responses) + "</D:multistatus>").encode()


def response(href, props, status="HTTP/1.1 200 OK"):
    return f"<D:response><D:href>{href}</D:href><D:propstat><D:prop>{props}</D:prop><D:status>{status}</D:status></D:propstat></D:response>"


COLLECTION = "<D:resourcetype><D:collection/></D:resourcetype>"


def test_object():
    body = multistatus(
        response(
            "/foo/bar/file1.txt",
            "<D:getcontentlength>2262</D:getcontentlength><D:getlastmodified>Tue, 25 Feb 2025 16:45:43 GMT</D:getlastmodified><D:getcontenttype>text/plain</D:getcontenttype><D:resourcetype/>",
        )
    )
    assert parse_multistatus(body) == [Resource(path="/foo/bar/file1.txt", is_collection=False, size=2262, modified=datetime(2025, 2, 25, 16, 45, 43, tzinfo=UTC), content_type="text/plain")]


def test_collection_gets_a_trailing_slash_and_no_size():
    body = multistatus(response("/foo/bar", f"{COLLECTION}<D:getcontentlength>4096</D:getcontentlength>"))
    assert parse_multistatus(body) == [Resource(path="/foo/bar/", is_collection=True, size=None)]


def test_top_listing(top_listing_response):
    resources = parse_multistatus(top_listing_response.encode(), "/foo/bar/")
    assert [resource.path for resource in resources] == ["/foo/bar/", "/foo/bar/file1.txt", "/foo/bar/file2.md", "/foo/bar/file3.txt", "/foo/bar/folder1/", "/foo/bar/folder2/"]


def test_href_is_decoded():
    body = multistatus(response("/foo/a%20b/100%25.txt", "<D:getcontentlength>1</D:getcontentlength>"))
    assert parse_multistatus(body)[0].path == "/foo/a b/100%.txt"


def test_absolute_href():
    body = multistatus(response("https://origin.example.org:8443/foo/a%20b/", COLLECTION))
    assert parse_multistatus(body)[0].path == "/foo/a b/"


def test_relative_href_is_resolved_against_the_request_path():
    body = multistatus(response("file1.txt", ""), response("sub/", COLLECTION))
    assert [resource.path for resource in parse_multistatus(body, "/foo/bar/")] == ["/foo/bar/file1.txt", "/foo/bar/sub/"]
    assert [resource.path for resource in parse_multistatus(body, "/foo/bar")] == ["/foo/file1.txt", "/foo/sub/"]


def test_colon_in_a_name_is_not_a_scheme():
    body = multistatus(response("/foo/time:12:00.txt", ""))
    assert parse_multistatus(body)[0].path == "/foo/time:12:00.txt"


def test_failed_propstat_is_ignored():
    """A property the server could not supply sits in a propstat with an error status."""
    body = multistatus(
        "<D:response><D:href>/foo/bar/file1.txt</D:href>"
        "<D:propstat><D:prop><D:getcontentlength>5</D:getcontentlength></D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat>"
        "<D:propstat><D:prop><D:getcontenttype>text/plain</D:getcontenttype><D:getlastmodified>Tue, 25 Feb 2025 16:45:43 GMT</D:getlastmodified></D:prop>"
        "<D:status>HTTP/1.1 404 Not Found</D:status></D:propstat>"
        "</D:response>"
    )
    assert parse_multistatus(body) == [Resource(path="/foo/bar/file1.txt", is_collection=False, size=5, modified=None, content_type=None)]


def test_propstat_without_status_counts_as_successful():
    body = multistatus("<D:response><D:href>/foo/bar/file1.txt</D:href><D:propstat><D:prop><D:getcontentlength>5</D:getcontentlength></D:prop></D:propstat></D:response>")
    assert parse_multistatus(body)[0].size == 5


def test_properties_directly_under_the_response():
    body = multistatus("<D:response><D:href>/foo/bar/</D:href><D:prop><D:resourcetype><D:collection/></D:resourcetype></D:prop></D:response>")
    assert parse_multistatus(body)[0].is_collection is True


def test_response_without_href_is_skipped():
    body = multistatus("<D:response><D:propstat><D:prop/><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response>", response("/foo/bar/file1.txt", ""))
    assert [resource.path for resource in parse_multistatus(body)] == ["/foo/bar/file1.txt"]


def test_other_namespaces_and_prefixes(top_listing_response):
    """The pelicanfs fixture uses lp1: for DAV: properties and carries Apache and LCGDM namespaces besides."""
    resources = parse_multistatus(top_listing_response.encode())
    assert resources[0].is_collection is True
    assert resources[1].size == 2262


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("Tue, 25 Feb 2025 16:45:43 GMT", datetime(2025, 2, 25, 16, 45, 43, tzinfo=UTC)),
        ("Tue, 25 Feb 2025 16:45:43 +0100", datetime(2025, 2, 25, 16, 45, 43, tzinfo=timezone(timedelta(hours=1)))),
        ("Tue, 25 Feb 2025 16:45:43 -0000", datetime(2025, 2, 25, 16, 45, 43, tzinfo=UTC)),
        ("2024-01-01T00:00:00Z", datetime(2024, 1, 1, tzinfo=UTC)),
        ("2024-01-01T00:00:00+02:00", datetime(2024, 1, 1, tzinfo=timezone(timedelta(hours=2)))),
        ("2024-01-01T00:00:00", datetime(2024, 1, 1, tzinfo=UTC)),
        ("  Tue, 25 Feb 2025 16:45:43 GMT  ", datetime(2025, 2, 25, 16, 45, 43, tzinfo=UTC)),
        ("yesterday", None),
        ("", None),
    ],
)
def test_modified(value, expected):
    body = multistatus(response("/foo/bar/file1.txt", f"<D:getlastmodified>{value}</D:getlastmodified>"))
    assert parse_multistatus(body)[0].modified == expected


@pytest.mark.parametrize(("value", "expected"), [("13", 13), ("0", 0), ("", None), ("lots", None)])
def test_size(value, expected):
    body = multistatus(response("/foo/bar/file1.txt", f"<D:getcontentlength>{value}</D:getcontentlength>"))
    assert parse_multistatus(body)[0].size == expected


def test_missing_properties():
    assert parse_multistatus(multistatus(response("/foo/bar/file1.txt", ""))) == [Resource(path="/foo/bar/file1.txt", is_collection=False)]


def test_empty_multistatus():
    assert parse_multistatus(multistatus()) == []


@pytest.mark.parametrize(
    ("body", "message"),
    [
        (b"hello, world!", "not well-formed XML"),
        (b"", "not well-formed XML"),
        (b"<D:multistatus xmlns:D='DAV:'><D:response>", "not well-formed XML"),
        (b"<html><body>Not Found</body></html>", "root element is html"),
        (b"<multistatus><response><href>/x</href></response></multistatus>", "root element is multistatus, not {DAV:}multistatus"),
    ],
)
def test_not_a_multistatus(body, message):
    with pytest.raises(MultistatusError, match=message):
        parse_multistatus(body)


def test_doctype_is_rejected():
    """A DTD can define entities that expand without bound; a multistatus never needs one."""
    bomb = (
        b'<?xml version="1.0"?><!DOCTYPE x [<!ENTITY a "aaaaaaaaaa"><!ENTITY b "&a;&a;&a;&a;&a;&a;&a;&a;&a;&a;">]>'
        b'<D:multistatus xmlns:D="DAV:"><D:response><D:href>&b;</D:href></D:response></D:multistatus>'
    )
    with pytest.raises(MultistatusError, match="document type declarations are not allowed"):
        parse_multistatus(bomb)

    external = b'<!DOCTYPE multistatus SYSTEM "http://example.org/x.dtd"><D:multistatus xmlns:D="DAV:"/>'
    with pytest.raises(MultistatusError, match="document type declarations are not allowed"):
        parse_multistatus(external)


def test_same_path():
    assert same_path("/foo/bar", "/foo/bar/")
    assert same_path("/", "/")
    assert same_path("", "/")
    assert not same_path("/foo/bar", "/foo/baz")
    assert not same_path("/foo/bar", "/foo")


@pytest.mark.parametrize(
    ("path", "expected"),
    [("/foo/bar/file1.txt", "file1.txt"), ("/foo/bar/", "bar"), ("/foo", "foo"), ("/", "")],
)
def test_name(path, expected):
    assert Resource(path=path, is_collection=path.endswith("/")).name == expected


@pytest.mark.parametrize(
    ("path", "base", "expected"),
    [
        ("/api/data/ns/obj", "/api/data", "/ns/obj"),
        ("/api/data/ns/", "/api/data/", "/ns/"),
        ("/api/data/", "/api/data", "/"),
        ("/api/data", "/api/data", "/"),
        ("/api/database/obj", "/api/data", "/api/database/obj"),
        ("/elsewhere/obj", "/api/data", "/elsewhere/obj"),
        ("/ns/obj", "", "/ns/obj"),
        ("/ns/obj", "/", "/ns/obj"),
    ],
)
def test_relative_to(path, base, expected):
    assert Resource(path=path, is_collection=False).relative_to(base).path == expected
