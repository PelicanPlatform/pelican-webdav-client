# pelican-webdav-client

A PROPFIND-only asynchronous WebDAV client for the [Pelican Platform](https://pelicanplatform.org).

[PelicanFS](https://github.com/PelicanPlatform/pelicanfs) uses WebDAV for exactly one thing:
PROPFIND, to list a collection and to tell an object from a collection. This package does
that and nothing else, on top of [aiohttp](https://docs.aiohttp.org/), so that the WebDAV
half of PelicanFS depends on code the Pelican project controls.

- Python 3.11 and later; `aiohttp` and `yarl` are the only dependencies.
- Paths are taken percent-decoded and encoded by the client, so a `%` in a name is a
  literal percent sign. Paths reported back are relative to the client's base URL, so a
  client rooted at an origin's data endpoint speaks namespace paths throughout.
- A redirect is followed and the reply is read against the URL that finally answered,
  so a server that redirects one form of a URL to another still describes the resource
  asked about.
- Every response is read or released before a call returns or raises, so a client never
  holds a connection between calls.
- Multistatus bodies are parsed with the standard library; a document type declaration is
  rejected outright, which is how entity-expansion attacks would arrive.

## Install

```
pip install pelican-webdav-client
```

## Use

```python
import asyncio

from pelican_webdav_client import Client, NotFoundError


async def main():
    async with Client("https://origin.example.org", token="...") as client:
        # The resources in a collection, without the collection itself
        for resource in await client.list("/namespace/collection"):
            print(resource.path, resource.is_collection, resource.size, resource.modified)

        # One resource
        try:
            resource = await client.stat("/namespace/collection/object")
        except NotFoundError:
            print("no such object")
        else:
            print(resource.content_type)

        # Existence by status alone, without parsing the reply
        print(await client.exists("/namespace/collection/object"))

        # The raw PROPFIND, every entry the server describes
        print(await client.propfind("/namespace/collection", depth=1))


asyncio.run(main())
```

### `Client(base_url, *, token=None, ssl=True, session=None, timeout=None)`

`base_url` is the server, with any path prefix the resources live under; paths given to
the methods are appended to it and paths reported back have it removed. `token` is sent
as a bearer token on every request. `ssl` and `timeout` are passed to aiohttp unchanged:
`ssl` takes `True`, `False` or an `ssl.SSLContext`, for example one that trusts a private
certificate authority. `session` is an `aiohttp.ClientSession` to send requests through;
without one the client creates its own on first use and closes it in `close()`, and a
session passed in is left to its owner.

| Method | Request | Returns |
|---|---|---|
| `propfind(path, *, depth=0)` | `PROPFIND path`, `Depth: 0`, `1` or `infinity` | every resource in the reply, as `list[Resource]` |
| `stat(path)` | `PROPFIND path`, `Depth: 0` | the `Resource` for `path`; `NotFoundError` if the reply does not describe it |
| `list(path)` | `PROPFIND path/`, `Depth: 1` | the resources in the collection, without the collection itself |
| `is_collection(path)` | as `stat` | `bool` |
| `exists(path)` | `PROPFIND path`, `Depth: 0` | `True` on any success status, `False` on 404; the body is not read |
| `close()` | | closes the session, if the client created it |

`list` always asks for the collection's slash form. The other methods send the path as
given, trailing slash or not, because a server may answer the two forms differently:
Pelican origins answer a collection's slash form and reject an object's.

### `Resource`

A frozen dataclass with `path` (decoded, absolute, relative to the client's base URL;
collections end with `/`), `name` (the final component of the path), `is_collection`,
`size` (`None` for collections), `modified` (an aware `datetime`, or `None`) and
`content_type`. A path that a redirect carried outside the base URL is reported whole.

### Errors

```
WebDavError
├── ResponseError            .method .url .status .reason .body
│   ├── UnauthorizedError    401
│   ├── ForbiddenError       403
│   ├── NotFoundError        404, or a reply that does not describe the path asked about
│   └── MethodNotAllowedError 405
└── InvalidResponseError     a success whose body is not a multistatus document
```

Transport failures are aiohttp's own exceptions and are not wrapped.

## How PelicanFS uses it

PelicanFS builds one client per operation from the collections endpoint's host, the
bearer token it has discovered for the namespace, and the `verify_ssl` option:

```python
@asynccontextmanager
async def get_webdav_client(options):
    async with Client(options["hostname"], token=options["token"], ssl=options.get("verify_ssl", True)) as client:
        yield client
```

Then `ls` is `client.list`, `info` is `client.stat`, `isdir` is `client.is_collection` and
`exists` is `client.exists`. A `ResponseError` with status 500 from the slash form of a path
is how an origin says the path is an object, not a collection, and PelicanFS keys on
`error.status` to tell that apart from a real failure.

On an XRootD origin the object URL is the host plus the namespace path, so a host-only
`base_url` is enough. A Go origin serves objects under an endpoint prefix such as
`/api/v1.0/origin/data`, which the director reports as the `collections-url`. Rooting the
client there, and passing namespace paths, keeps that prefix out of every path the client
reports, which is what lets `ls` and `info` name entries by paths a caller can hand back
to `open()`:

```python
# hostname carries the endpoint prefix, e.g. https://origin.example.org:8443/api/v1.0/origin/data
async with Client(options["hostname"], token=options["token"], ssl=options.get("verify_ssl", True)) as client:
    entries = await client.list("/namespace/collection")  # paths come back as /namespace/collection/...
```

## Development

```
pip install -e . --group testing
pytest
```

The tests run against [pytest-httpserver](https://pytest-httpserver.readthedocs.io/) and
treat every warning as an error, so a session or connection left open fails the run.

Linting, formatting and type checking are pre-commit hooks, run in CI with
[prek](https://github.com/j178/prek) and runnable the same way locally:

```
pip install prek
prek run --all-files
```

The tooling mirrors [pelicanfs](https://github.com/PelicanPlatform/pelicanfs): ruff with
the same rule set, pyright, zizmor over the workflows, a test matrix across the supported
Pythons and operating systems, and trusted publishing to TestPyPI and PyPI on a GitHub
release whose tag matches the version in `pyproject.toml`.
