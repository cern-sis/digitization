import json

import pytest


class FakeResponse:
    def __init__(self, body=None, status=200):
        self.body = body if body is not None else {}
        self.status_code = status
        self.text = json.dumps(self.body)

    def json(self):
        return self.body

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class FakeSession:
    """Returns the queued responses for each (method, url), in order, and records the calls."""

    def __init__(self, responses):
        self.responses = {key: list(bodies) for key, bodies in responses.items()}
        self.calls = []

    def request(self, method, url, **kwargs):
        self.calls.append({"method": method, "url": url, **kwargs})
        queue = self.responses.get((method, url))
        if not queue:
            raise AssertionError(f"Unexpected request: {method} {url}")
        return queue.pop(0)


@pytest.fixture
def fake_session():
    return FakeSession


@pytest.fixture
def response():
    return FakeResponse
