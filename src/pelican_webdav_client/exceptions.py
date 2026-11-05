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

# The errors this package raises. Transport failures (connection refused, TLS handshake
# failures, timeouts) are aiohttp's own exceptions and are not wrapped.

from __future__ import annotations


class WebDavError(Exception):
    """
    Base class for every error this package raises.
    """


class ResponseError(WebDavError):
    """
    The server answered a request with an error status.

    `status` is the HTTP status code and `body` the decoded response body, which is what a
    server usually puts its explanation in. The 401, 403, 404 and 405 statuses raise the
    subclasses below, so a caller can single those out without comparing codes.
    """

    def __init__(self, method: str, url: str, status: int, reason: str = "", body: str = "") -> None:
        super().__init__(f"{method} {url} failed with status {status} {reason}".rstrip())
        self.method = method
        self.url = url
        self.status = status
        self.reason = reason
        self.body = body

    @classmethod
    def for_status(cls, method: str, url: str, status: int, reason: str = "", body: str = "") -> ResponseError:
        """
        The error for `status`: a specific subclass where there is one, otherwise this class.
        """
        error_class = _STATUS_CLASSES.get(status, cls)
        return error_class(method, url, status, reason, body)


class UnauthorizedError(ResponseError):
    """The server answered 401: the request carried no acceptable credentials."""


class ForbiddenError(ResponseError):
    """The server answered 403: the credentials do not allow this request."""


class NotFoundError(ResponseError):
    """
    The server answered 404, or a successful reply did not describe the requested path.

    The second case comes from `Client.stat`, which asks about one path and gets back a
    multistatus that names other resources or none. `status` then holds the reply's actual
    status and `reason` says what was missing.
    """


class MethodNotAllowedError(ResponseError):
    """The server answered 405: it does not support PROPFIND on this resource."""


class InvalidResponseError(WebDavError):
    """
    The server answered successfully but the body is not a multistatus document.
    """

    def __init__(self, method: str, url: str, message: str) -> None:
        super().__init__(f"{method} {url} returned an invalid response: {message}")
        self.method = method
        self.url = url
        self.message = message


_STATUS_CLASSES: dict[int, type[ResponseError]] = {
    401: UnauthorizedError,
    403: ForbiddenError,
    404: NotFoundError,
    405: MethodNotAllowedError,
}
