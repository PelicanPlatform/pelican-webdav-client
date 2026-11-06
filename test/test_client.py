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
from datetime import UTC, datetime

import aiohttp
import pytest

from pelican_webdav_client import Client, InvalidResponseError, NotFoundError, Resource


def test_list(httpserver, top_listing_response):
    """A listing goes to the collection's slash form with Depth 1 and leaves the collection out."""
    httpserver.expect_request("/foo/bar/", method="PROPFIND", headers={"Depth": "1"}).respond_with_data(top_listing_response, status=207, content_type="application/xml")

    async def main():
        async with Client(httpserver.url_for("/")) as client:
            return await client.list("/foo/bar")

    listing = asyncio.run(main())

    assert [resource.path for resource in listing] == ["/foo/bar/file1.txt", "/foo/bar/file2.md", "/foo/bar/file3.txt", "/foo/bar/folder1/", "/foo/bar/folder2/"]
    assert [resource.is_collection for resource in listing] == [False, False, False, True, True]
    assert [resource.size for resource in listing] == [2262, 136, 319, None, None]
    assert all(resource.modified == datetime(2025, 2, 25, 16, 45, 43, tzinfo=UTC) for resource in listing)


def test_list_with_trailing_slash(httpserver, top_listing_response):
    httpserver.expect_request("/foo/bar/", method="PROPFIND", headers={"Depth": "1"}).respond_with_data(top_listing_response, status=207)

    async def main():
        async with Client(httpserver.url_for("/")) as client:
            return await client.list("/foo/bar/")

    assert len(asyncio.run(main())) == 5


def test_list_empty_collection(httpserver):
    """A collection that lists only itself is empty, not missing."""
    body = (
        '<?xml version="1.0"?><D:multistatus xmlns:D="DAV:"><D:response><D:href>/foo/empty/</D:href>'
        "<D:propstat><D:prop><D:resourcetype><D:collection/></D:resourcetype></D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response></D:multistatus>"
    )
    httpserver.expect_request("/foo/empty/", method="PROPFIND").respond_with_data(body, status=207)

    async def main():
        async with Client(httpserver.url_for("/")) as client:
            return await client.list("/foo/empty")

    assert asyncio.run(main()) == []


def test_stat(httpserver, file1_listing_response):
    """A stat goes to the path as given, with Depth 0, and returns the entry for it."""
    httpserver.expect_request("/foo/bar/file1.txt", method="PROPFIND", headers={"Depth": "0"}).respond_with_data(file1_listing_response, status=207)

    async def main():
        async with Client(httpserver.url_for("/")) as client:
            return await client.stat("/foo/bar/file1.txt")

    assert asyncio.run(main()) == Resource(path="/foo/bar/file1.txt", is_collection=False, size=5, modified=None, content_type=None)


def test_stat_collection_named_with_slash(httpserver, top_listing_response):
    """The entry is matched with trailing slashes ignored, and a collection's path ends with one."""
    httpserver.expect_request("/foo/bar", method="PROPFIND", headers={"Depth": "0"}).respond_with_data(top_listing_response, status=207)

    async def main():
        async with Client(httpserver.url_for("/")) as client:
            return await client.stat("/foo/bar")

    resource = asyncio.run(main())
    assert resource.path == "/foo/bar/"
    assert resource.is_collection is True
    assert resource.size is None


def test_stat_reply_without_the_path(httpserver, file1_listing_response):
    """A successful reply that does not describe the path is a NotFoundError carrying the reply's status."""
    httpserver.expect_request("/foo/bar/other.txt", method="PROPFIND").respond_with_data(file1_listing_response, status=207)

    async def main():
        async with Client(httpserver.url_for("/")) as client:
            return await client.stat("/foo/bar/other.txt")

    with pytest.raises(NotFoundError) as excinfo:
        asyncio.run(main())
    assert excinfo.value.status == 207
    assert "/foo/bar/other.txt" in excinfo.value.reason


def test_stat_matches_absolute_href(httpserver):
    body = (
        f'<?xml version="1.0"?><D:multistatus xmlns:D="DAV:"><D:response><D:href>{httpserver.url_for("/foo/a%20b")}</D:href>'
        "<D:propstat><D:prop><D:getcontentlength>7</D:getcontentlength><D:getcontenttype>text/plain</D:getcontenttype></D:prop>"
        "<D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response></D:multistatus>"
    )
    httpserver.expect_request("/foo/a b", method="PROPFIND").respond_with_data(body, status=207)

    async def main():
        async with Client(httpserver.url_for("/")) as client:
            return await client.stat("/foo/a b")

    assert asyncio.run(main()) == Resource(path="/foo/a b", is_collection=False, size=7, content_type="text/plain")


def test_is_collection(httpserver, top_listing_response, file1_listing_response):
    httpserver.expect_request("/foo/bar/", method="PROPFIND", headers={"Depth": "0"}).respond_with_data(top_listing_response, status=207)
    httpserver.expect_request("/foo/bar/file1.txt", method="PROPFIND", headers={"Depth": "0"}).respond_with_data(file1_listing_response, status=207)

    async def main():
        async with Client(httpserver.url_for("/")) as client:
            return await client.is_collection("/foo/bar/"), await client.is_collection("/foo/bar/file1.txt")

    assert asyncio.run(main()) == (True, False)


def test_propfind_returns_every_entry(httpserver, top_listing_response):
    httpserver.expect_request("/foo/bar/", method="PROPFIND", headers={"Depth": "infinity"}).respond_with_data(top_listing_response, status=207)

    async def main():
        async with Client(httpserver.url_for("/")) as client:
            return await client.propfind("/foo/bar/", depth="infinity")

    assert [resource.path for resource in asyncio.run(main())][:2] == ["/foo/bar/", "/foo/bar/file1.txt"]


def test_propfind_default_depth_is_zero(httpserver, file1_listing_response):
    httpserver.expect_request("/foo/bar/file1.txt", method="PROPFIND").respond_with_data(file1_listing_response, status=207)

    async def main():
        async with Client(httpserver.url_for("/")) as client:
            return await client.propfind("/foo/bar/file1.txt")

    asyncio.run(main())
    assert httpserver.log[-1][0].headers["Depth"] == "0"


def test_propfind_accepts_200(httpserver, file1_listing_response):
    """Any success status will do; some servers answer 200 rather than 207."""
    httpserver.expect_request("/foo/bar/file1.txt", method="PROPFIND").respond_with_data(file1_listing_response, status=200)

    async def main():
        async with Client(httpserver.url_for("/")) as client:
            return await client.propfind("/foo/bar/file1.txt")

    assert len(asyncio.run(main())) == 1


def test_propfind_rejects_a_body_that_is_not_multistatus(httpserver):
    httpserver.expect_request("/foo/bar", method="PROPFIND").respond_with_data("hello, world!", status=200)

    async def main():
        async with Client(httpserver.url_for("/")) as client:
            return await client.propfind("/foo/bar")

    with pytest.raises(InvalidResponseError, match="not well-formed XML"):
        asyncio.run(main())


def test_exists_true_on_any_success(httpserver):
    """exists() judges by the status alone, so a success with a body that is not even XML counts."""
    httpserver.expect_request("/foo/bar", method="PROPFIND", headers={"Depth": "0"}).respond_with_data("hello, world!", status=200)

    async def main():
        async with Client(httpserver.url_for("/")) as client:
            return await client.exists("/foo/bar")

    assert asyncio.run(main()) is True


def test_exists_false_on_404(httpserver):
    httpserver.expect_request("/foo/bar", method="PROPFIND").respond_with_data("", status=404)

    async def main():
        async with Client(httpserver.url_for("/")) as client:
            return await client.exists("/foo/bar")

    assert asyncio.run(main()) is False


def test_exists_keeps_the_path_as_given(httpserver, top_listing_response):
    """Unlike list(), exists() and stat() do not add a slash: the server tells the two forms apart."""
    httpserver.expect_request("/foo/bar", method="PROPFIND").respond_with_data(top_listing_response, status=207)
    httpserver.expect_request("/foo/bar/", method="PROPFIND").respond_with_data(top_listing_response, status=207)

    async def main():
        async with Client(httpserver.url_for("/")) as client:
            await client.exists("/foo/bar")
            first = httpserver.log[-1][0].path
            await client.exists("/foo/bar/")
            return first, httpserver.log[-1][0].path

    assert asyncio.run(main()) == ("/foo/bar", "/foo/bar/")


def test_client_reused_across_calls(httpserver, top_listing_response, file1_listing_response):
    """One client serves many calls in turn, as pelicanfs's walk does."""
    httpserver.expect_request("/foo/bar/", method="PROPFIND").respond_with_data(top_listing_response, status=207)
    httpserver.expect_request("/foo/bar/file1.txt", method="PROPFIND").respond_with_data(file1_listing_response, status=207)

    async def main():
        async with Client(httpserver.url_for("/")) as client:
            return [len(await client.list("/foo/bar")), (await client.stat("/foo/bar/file1.txt")).size, await client.exists("/foo/bar/file1.txt"), await client.is_collection("/foo/bar/")]

    assert asyncio.run(main()) == [5, 5, True, True]


def test_stat_follows_a_redirect_and_matches_the_answered_path(httpserver):
    """
    A server may redirect one form of a resource's URL to another, as a Pelican director
    does for an origin's data endpoint. The reply names the final URL's path, and that is
    what the requested resource is matched by; the path asked for is only how to get there.
    """
    body = (
        '<?xml version="1.0"?><D:multistatus xmlns:D="DAV:">'
        "<D:response><D:href>/api/v1.0/origin/data/ns/obj</D:href>"
        "<D:propstat><D:prop><D:getcontentlength>13</D:getcontentlength></D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response>"
        "</D:multistatus>"
    )
    httpserver.expect_request("/ns/obj", method="PROPFIND").respond_with_data("", status=307, headers={"Location": httpserver.url_for("/api/v1.0/origin/data/ns/obj")})
    httpserver.expect_request("/api/v1.0/origin/data/ns/obj", method="PROPFIND", headers={"Depth": "0"}).respond_with_data(body, status=207)

    async def main():
        async with Client(httpserver.url_for("/")) as client:
            return await client.stat("/ns/obj")

    assert asyncio.run(main()) == Resource(path="/api/v1.0/origin/data/ns/obj", is_collection=False, size=13)


def test_list_follows_a_redirect_and_drops_the_collection_by_its_answered_path(httpserver):
    body = (
        '<?xml version="1.0"?><D:multistatus xmlns:D="DAV:">'
        "<D:response><D:href>/api/v1.0/origin/data/ns/subdir/</D:href>"
        "<D:propstat><D:prop><D:resourcetype><D:collection/></D:resourcetype></D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response>"
        "<D:response><D:href>/api/v1.0/origin/data/ns/subdir/nested.txt</D:href>"
        "<D:propstat><D:prop><D:getcontentlength>7</D:getcontentlength></D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response>"
        "</D:multistatus>"
    )
    httpserver.expect_request("/ns/subdir/", method="PROPFIND").respond_with_data("", status=307, headers={"Location": httpserver.url_for("/api/v1.0/origin/data/ns/subdir/")})
    httpserver.expect_request("/api/v1.0/origin/data/ns/subdir/", method="PROPFIND", headers={"Depth": "1"}).respond_with_data(body, status=207)

    async def main():
        async with Client(httpserver.url_for("/")) as client:
            return await client.list("/ns/subdir")

    assert [resource.path for resource in asyncio.run(main())] == ["/api/v1.0/origin/data/ns/subdir/nested.txt"]


def test_paths_are_relative_to_the_base_url(httpserver):
    """
    A client rooted at an origin's data endpoint asks and answers in the paths beneath it,
    so an endpoint prefix such as Pelican's /api/v1.0/origin/data never reaches the caller.
    """
    body = (
        '<?xml version="1.0"?><D:multistatus xmlns:D="DAV:">'
        "<D:response><D:href>/api/v1.0/origin/data/ns/subdir/</D:href>"
        "<D:propstat><D:prop><D:resourcetype><D:collection/></D:resourcetype></D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response>"
        f"<D:response><D:href>{httpserver.url_for('/api/v1.0/origin/data/ns/subdir/nested.txt')}</D:href>"
        "<D:propstat><D:prop><D:getcontentlength>7</D:getcontentlength></D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response>"
        "<D:response><D:href>/api/v1.0/origin/data/ns/subdir/a%20b/</D:href>"
        "<D:propstat><D:prop><D:resourcetype><D:collection/></D:resourcetype></D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response>"
        "</D:multistatus>"
    )
    httpserver.expect_request("/api/v1.0/origin/data/ns/subdir/", method="PROPFIND").respond_with_data(body, status=207)

    async def main():
        async with Client(httpserver.url_for("/api/v1.0/origin/data")) as client:
            listing = await client.list("/ns/subdir")
            collection = await client.stat("/ns/subdir/")
            everything = await client.propfind("/ns/subdir/", depth=1)
            return listing, collection, everything

    listing, collection, everything = asyncio.run(main())
    assert [resource.path for resource in listing] == ["/ns/subdir/nested.txt", "/ns/subdir/a b/"]
    assert [resource.name for resource in listing] == ["nested.txt", "a b"]
    assert collection.path == "/ns/subdir/"
    assert collection.name == "subdir"
    assert [resource.path for resource in everything] == ["/ns/subdir/", "/ns/subdir/nested.txt", "/ns/subdir/a b/"]


def test_the_base_url_itself_is_the_root(httpserver):
    body = (
        '<?xml version="1.0"?><D:multistatus xmlns:D="DAV:">'
        "<D:response><D:href>/data/</D:href><D:propstat><D:prop><D:resourcetype><D:collection/></D:resourcetype></D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response>"
        "</D:multistatus>"
    )
    httpserver.expect_request("/data/", method="PROPFIND").respond_with_data(body, status=207)

    async def main():
        async with Client(httpserver.url_for("/data/")) as client:
            return await client.stat("/")

    resource = asyncio.run(main())
    assert resource.path == "/"
    assert resource.name == ""


def test_a_redirect_loop_ends_in_too_many_redirects(httpserver):
    """The client follows redirects itself, so it also enforces aiohttp's limit on them."""
    httpserver.expect_request("/loop", method="PROPFIND").respond_with_data("", status=307, headers={"Location": httpserver.url_for("/loop")})

    async def main():
        async with Client(httpserver.url_for("/")) as client:
            await client.stat("/loop")

    with pytest.raises(aiohttp.TooManyRedirects):
        asyncio.run(main())


def test_a_redirect_to_another_origin_drops_the_token(httpserver, threaded_httpserver, top_listing_response):
    """As aiohttp does: a bearer token is for the server it was given to, not for wherever that server points."""
    threaded_httpserver.clear()
    httpserver.expect_request("/foo/bar/", method="PROPFIND", headers={"Authorization": "Bearer secret"}).respond_with_data(
        "", status=307, headers={"Location": threaded_httpserver.url_for("/foo/bar/")}
    )
    threaded_httpserver.expect_request("/foo/bar/", method="PROPFIND").respond_with_data(top_listing_response, status=207, content_type="application/xml")

    async def main():
        async with Client(httpserver.url_for("/"), token="secret") as client:
            return await client.is_collection("/foo/bar/")

    assert asyncio.run(main()) is True
    assert len(threaded_httpserver.log) == 1
    assert "Authorization" not in threaded_httpserver.log[0][0].headers


def test_a_path_outside_the_base_is_reported_whole(httpserver):
    """A redirect can land outside the base; such a path is reported as the server named it."""
    body = (
        '<?xml version="1.0"?><D:multistatus xmlns:D="DAV:">'
        "<D:response><D:href>/elsewhere/obj</D:href><D:propstat><D:prop><D:getcontentlength>1</D:getcontentlength></D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response>"
        "</D:multistatus>"
    )
    httpserver.expect_request("/data/obj", method="PROPFIND").respond_with_data("", status=307, headers={"Location": httpserver.url_for("/elsewhere/obj")})
    httpserver.expect_request("/elsewhere/obj", method="PROPFIND").respond_with_data(body, status=207)

    async def main():
        async with Client(httpserver.url_for("/data")) as client:
            return await client.stat("/obj")

    assert asyncio.run(main()).path == "/elsewhere/obj"
