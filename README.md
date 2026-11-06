# pelican-webdav-client

An asynchronous WebDAV client for the [Pelican Platform](https://github.com/PelicanPlatform/pelican) that speaks `PROPFIND` and nothing else.

It exists to serve [PelicanFS](https://github.com/PelicanPlatform/pelicanfs), which uses WebDAV only to list collections and to tell objects from collections. It does not upload, download, copy, move, delete or lock anything. If you need those, use a general-purpose WebDAV client.

- Built on `aiohttp`, with no other runtime dependencies
- XML is parsed with the standard library, and documents that carry a DTD are rejected
- Requires Python 3.11 or later
- Fully typed (`py.typed`)

## Installation

```sh
pip install pelican-webdav-client
```

## Quick start

```python
import asyncio

from pelican_webdav_client import Client, NotFoundError


async def main() -> None:
    async with Client("https://origin.example.org", token="...") as client:
        for resource in await client.list("/namespace/collection"):
            print(resource.name, resource.is_collection, resource.size, resource.modified)

        try:
            info = await client.stat("/namespace/collection/file.txt")
        except NotFoundError:
            print("no such object")
        else:
            print(info.path, info.size, info.content_type)


asyncio.run(main())
```

## The client

```python
Client(
    base_url: str,
    *,
    token: str | None = None,
    ssl: bool | ssl.SSLContext = True,
    session: aiohttp.ClientSession | None = None,
    timeout: aiohttp.ClientTimeout | None = None,
)
```

| Argument   | Meaning |
|------------|---------|
| `base_url` | The server, as an absolute URL, plus any path prefix the resources live under. |
| `token`    | Sent as `Authorization: Bearer <token>` on every request. |
| `ssl`      | Passed to aiohttp unchanged: `True` verifies certificates, `False` skips verification, or pass your own `ssl.SSLContext` (for example, one that trusts a private CA). |
| `session`  | An aiohttp session to send requests through. If you pass one in, you own it and the client leaves it open. If you don't, the client creates its own on first use and closes it in `close()`. |
| `timeout`  | An `aiohttp.ClientTimeout` for each request. Defaults to the session's timeout. |

`Client` is an async context manager, and leaving the `async with` block calls `close()`.

### Methods

| Method | Returns | What it does |
|--------|---------|--------------|
| `list(path)` | `list[Resource]` | The members of the collection at `path`, without the collection itself. Sends `Depth: 1` to the collection's slash form, so a trailing slash on `path` is optional. |
| `stat(path)` | `Resource` | What the server says about the resource at `path`. Raises `NotFoundError` on a 404, and also when the reply doesn't describe `path`. |
| `is_collection(path)` | `bool` | Shorthand for `(await stat(path)).is_collection`. |
| `exists(path)` | `bool` | Judged from the status code alone: a 404 gives `False`, any success gives `True`, and any other error raises. Unlike `stat`, it doesn't care what the body says. |
| `propfind(path, *, depth=0)` | `list[Resource]` | The raw result: every resource the server describes in the reply, including the target itself. `depth` is `0`, `1` or `"infinity"`. |
| `url(path)` | `yarl.URL` | The URL a request for `path` is sent to. |

Every method reads or releases its response before it returns or raises, so the client never holds a connection open between calls.

### Paths

- **Paths go in decoded.** Pass paths as the server should see them, and the client percent-encodes them. `"a b"` is sent as `a%20b`, and a `%` in a path is a literal percent sign (`"100%"` is sent as `100%25`). A missing leading slash is added for you.
- **Paths are relative to `base_url`.** The client adds `base_url`'s path prefix to each request and strips it from each `Resource.path` it returns. If you root the client at an origin's data endpoint, you can work entirely in namespace paths. A resource the server reports outside the prefix keeps its full path.
- **Trailing slashes matter.** Pelican origins answer a collection's slash form and reject an object's. `stat`, `exists`, `is_collection` and `propfind` send `path` exactly as given, while `list` always adds the slash.
- **Redirects are followed.** The reply is matched against the URL that finally answered, so a server that redirects one form of a URL to another still gets a correct `stat` or `list`.

## `Resource`

A frozen dataclass describing one entry in a `PROPFIND` reply.

| Field | Type | Notes |
|-------|------|-------|
| `path` | `str` | Decoded and relative to `base_url`. Collections end in `/` and nothing else does. |
| `name` | `str` | The final path component, or `""` for the root (a property). |
| `is_collection` | `bool` | From `DAV:resourcetype`. |
| `size` | `int \| None` | From `DAV:getcontentlength`. It is always `None` for a collection, and `None` when the server sent no usable length. |
| `modified` | `datetime \| None` | From `DAV:getlastmodified`, as a timezone-aware datetime. RFC 1123 and ISO 8601 dates are both accepted, and a date with no zone is taken as UTC. |
| `content_type` | `str \| None` | From `DAV:getcontenttype`. |

The parser only reads properties from `propstat` blocks with a 2xx status. It accepts `href`s that are relative, path-absolute or full URLs.

## Errors

```
WebDavError
├── ResponseError            the server answered with a 4xx or 5xx status
│   ├── UnauthorizedError    401
│   ├── ForbiddenError       403
│   ├── NotFoundError        404, or a stat() reply that doesn't describe the path
│   └── MethodNotAllowedError 405
└── InvalidResponseError     a successful reply whose body is not a DAV:multistatus document
```

A `ResponseError` has `method`, `url`, `status`, `reason` and `body` attributes (`body` is the decoded response body, where servers usually explain themselves). An `InvalidResponseError` has `method`, `url` and `message`.

Transport failures such as refused connections, TLS errors and timeouts are not wrapped. They reach you as aiohttp's own exceptions.

## Logging

Requests and response statuses are logged at `DEBUG` level to the `pelican_webdav_client.client` logger. Tokens are never logged.

## Development

```sh
git clone https://github.com/PelicanPlatform/pelican-webdav-client
cd pelican-webdav-client
python -m venv .venv && source .venv/bin/activate
pip install -e . --group dev
pytest
```

The tests run against a local [`pytest-httpserver`](https://github.com/csernazs/pytest-httpserver), so they need no network access and no Pelican federation. Pytest is set to fail on any warning, which means an unclosed session or connection fails the test that leaked it.

Linting, formatting and type checking (ruff, pyright and zizmor for the workflows) run through pre-commit:

```sh
pre-commit install
pre-commit run --all-files
```

CI runs the same hooks, then runs the test suite on Python 3.11 through 3.15 on Linux, and on 3.11 and 3.15 on macOS and Windows.

## License

Apache License 2.0. See [LICENSE](LICENSE).
