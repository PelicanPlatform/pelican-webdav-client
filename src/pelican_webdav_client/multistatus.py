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

# Parsing of the multistatus body a PROPFIND returns (RFC 4918 section 9.1) into Resource
# records. Only the DAV: properties pelicanfs needs are read; everything else in the
# document is ignored.

from __future__ import annotations

import posixpath
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from urllib.parse import unquote, urlsplit
from xml.etree import ElementTree as ET

DAV = "{DAV:}"


@dataclass(frozen=True, slots=True, kw_only=True)
class Resource:
    """
    What a PROPFIND reply says about one resource.

    `path` is the resource's absolute path, percent-decoded, with a trailing slash on a
    collection and on nothing else. The parser reports the path exactly as the server
    named it in `<D:href>`; the client then makes it relative to its base URL, so that a
    client rooted at a server's data endpoint reports the paths beneath that endpoint.
    `name` is the final component of `path`, or "" for the root.

    `size` is None for a collection, whatever the server reported, and for a resource
    whose reply carried no usable content length. `modified` is timezone-aware, and None
    when the reply carried no date or one that could not be parsed.
    """

    path: str
    is_collection: bool
    size: int | None = None
    modified: datetime | None = None
    content_type: str | None = None

    @property
    def name(self) -> str:
        """The final component of `path`: the resource's own name within its collection."""
        return self.path.rstrip("/").rsplit("/", 1)[-1]

    def relative_to(self, base_path: str) -> Resource:
        """
        This resource with `base_path` removed from the front of its path.

        A path outside `base_path` is left alone. The root of `base_path` becomes "/".
        """
        base = base_path.rstrip("/")
        if not base:
            return self
        if self.path in (base, f"{base}/"):
            return replace(self, path="/")
        if self.path.startswith(f"{base}/"):
            return replace(self, path=self.path[len(base) :])
        return self


class MultistatusError(ValueError):
    """
    The body is not a multistatus document.
    """


def parse_multistatus(body: bytes, request_path: str = "/") -> list[Resource]:
    """
    The resources described by `body`, in document order.

    `request_path` is the decoded path the PROPFIND was sent to. A server may answer with
    hrefs relative to it, which are resolved against it; absolute hrefs, with or without a
    scheme and host, are taken as they are.

    Raises MultistatusError if the body is not well-formed XML, carries a document type
    declaration, or has no multistatus element at its root.
    """
    try:
        root = ET.fromstring(body, parser=_parser())
    except ET.ParseError as err:
        msg = f"not well-formed XML: {err}"
        raise MultistatusError(msg) from err
    except _DoctypeNotAllowed as err:
        raise MultistatusError(str(err)) from err

    if root.tag != f"{DAV}multistatus":
        msg = f"root element is {root.tag}, not {DAV}multistatus"
        raise MultistatusError(msg)

    resources = []
    for response in root.iter(f"{DAV}response"):
        resource = _resource(response, request_path)
        if resource is not None:
            resources.append(resource)
    return resources


def same_path(left: str, right: str) -> bool:
    """
    Whether two decoded paths name the same resource, ignoring a trailing slash.
    """
    return _comparison_key(left) == _comparison_key(right)


def _comparison_key(path: str) -> str:
    stripped = path.rstrip("/")
    return stripped or "/"


class _DoctypeNotAllowed(Exception):
    pass


class _TreeBuilder(ET.TreeBuilder):
    """
    A tree builder that rejects a document type declaration.

    A multistatus never needs a DTD, and a DTD is how entity-expansion attacks arrive. The
    parser reports a DOCTYPE to its target before it processes anything the declaration
    contains, so raising here rejects the document before an entity can be defined, let
    alone expanded.
    """

    def doctype(self, name: str, pubid: str | None, system: str | None) -> None:  # noqa: ARG002
        msg = "document type declarations are not allowed"
        raise _DoctypeNotAllowed(msg)


def _parser() -> ET.XMLParser:
    return ET.XMLParser(target=_TreeBuilder())


def _resource(response: ET.Element, request_path: str) -> Resource | None:
    href = response.findtext(f"{DAV}href")
    if href is None:
        return None

    props = _successful_props(response)
    is_collection = any(prop.find(f".//{DAV}resourcetype/{DAV}collection") is not None for prop in props)

    path = _href_path(href.strip(), request_path)
    if is_collection and not path.endswith("/"):
        path += "/"

    return Resource(
        path=path,
        is_collection=is_collection,
        size=None if is_collection else _int(_text(props, "getcontentlength")),
        modified=_datetime(_text(props, "getlastmodified")),
        content_type=_text(props, "getcontenttype"),
    )


def _successful_props(response: ET.Element) -> list[ET.Element]:
    """
    The <D:prop> elements of `response` whose propstat status is successful.

    A propstat with no status counts as successful. A response with no propstat at all is
    searched directly, for servers that put properties straight under the response.
    """
    propstats = response.findall(f"{DAV}propstat")
    if not propstats:
        return [response]

    props = []
    for propstat in propstats:
        if not _status_ok(propstat.findtext(f"{DAV}status")):
            continue
        prop = propstat.find(f"{DAV}prop")
        if prop is not None:
            props.append(prop)
    return props


def _status_ok(status: str | None) -> bool:
    if status is None:
        return True
    parts = status.split()
    if len(parts) < 2:
        return False
    try:
        code = int(parts[1])
    except ValueError:
        return False
    return 200 <= code < 300


def _href_path(href: str, request_path: str) -> str:
    """
    The decoded absolute path `href` names. Only an href with an authority is split; a
    bare path is taken whole, so a colon in a name is not read as a scheme.
    """
    parts = urlsplit(href)
    path = parts.path if parts.netloc else href
    path = unquote(path)
    if not path.startswith("/"):
        # Relative to the request-URI, per RFC 4918 section 8.3
        base = request_path if request_path.endswith("/") else posixpath.dirname(request_path) + "/"
        path = posixpath.normpath(base + path)
        if href.endswith("/") and not path.endswith("/"):
            path += "/"
    return path


def _text(props: list[ET.Element], name: str) -> str | None:
    for prop in props:
        value = prop.findtext(f".//{DAV}{name}")
        if value is not None:
            return value.strip() or None
    return None


def _int(value: str | None) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except ValueError:
        return None


def _datetime(value: str | None) -> datetime | None:
    """
    `value` as an aware datetime. Servers send RFC 1123 dates for getlastmodified, and
    some send ISO 8601; a date with no zone is taken as UTC.
    """
    if value is None:
        return None
    parsed = None
    for parse in (parsedate_to_datetime, datetime.fromisoformat):
        try:
            parsed = parse(value)
        except (ValueError, TypeError):
            continue
        break
    if parsed is None:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed
