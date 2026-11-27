"""HTTP helpers shared by the AtoM and repository.cern clients."""

from urllib.parse import urlparse

MAX_ERROR_BODY = 500


class HttpError(RuntimeError):
    """An HTTP error with the start of the response body (validation errors are there)."""

    def __init__(self, message: str, status_code: int):
        super().__init__(message)
        self.status_code = status_code


def check_response(response):
    """Returns the response, or raises HttpError with its body if it failed."""
    if response.status_code >= 400:
        url = getattr(response, "url", "")
        body = (response.text or "")[:MAX_ERROR_BODY]
        raise HttpError(
            f"HTTP {response.status_code} {url}: {body}", response.status_code
        )
    return response


def require_https(url: str, conn_id: str) -> str:
    """The credentials are sent with every request: never over plain HTTP."""
    if urlparse(url).scheme != "https":
        raise ValueError(f"The host of '{conn_id}' must start with https://: {url!r}")
    return url.rstrip("/")
