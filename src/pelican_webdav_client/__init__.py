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

A PROPFIND-only asynchronous WebDAV client for the Pelican Platform.

    async with Client("https://origin.example.org", token=token) as client:
        for resource in await client.list("/namespace/collection"):
            print(resource.path, resource.is_collection, resource.size)
"""

from .client import Client, Depth
from .exceptions import (
    ForbiddenError,
    InvalidResponseError,
    MethodNotAllowedError,
    NotFoundError,
    ResponseError,
    UnauthorizedError,
    WebDavError,
)
from .multistatus import Resource

__all__ = [
    "Client",
    "Depth",
    "ForbiddenError",
    "InvalidResponseError",
    "MethodNotAllowedError",
    "NotFoundError",
    "Resource",
    "ResponseError",
    "UnauthorizedError",
    "WebDavError",
]
