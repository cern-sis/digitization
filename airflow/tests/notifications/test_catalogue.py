from datetime import date

import pytest

from notifications import catalogue, config


class FakeResponse:
    def __init__(self, body, status=200):
        self.body = body
        self.status = status

    def json(self):
        return self.body

    def raise_for_status(self):
        if self.status >= 400:
            raise RuntimeError(f"HTTP {self.status}")


class FakeSession:
    """Returns the queued responses for each URL, in order, and records the calls."""

    def __init__(self, responses):
        self.responses = {url: list(bodies) for url, bodies in responses.items()}
        self.calls = []

    def get(self, url, params=None, headers=None, timeout=None):
        self.calls.append({"url": url, "params": params, "headers": headers})
        return self.responses[url].pop(0)


def page(hits, next_url=None):
    body = {"hits": {"hits": hits, "total": len(hits)}, "links": {}}
    if next_url:
        body["links"]["next"] = next_url
    return FakeResponse(body)


def item(document_pid):
    return {"metadata": {"document_pid": document_pid}}


def literature_hit(pid, title="A title", subjects=None):
    return {
        "id": pid,
        "metadata": {
            "pid": pid,
            "title": title,
            "document_type": "BOOK",
            "publication_year": "2024",
            "subjects": subjects or [],
            "authors": [{"full_name": "Someone"}],
        },
    }


def test_created_range_query_excludes_end():
    assert (
        catalogue.created_range_query(date(2026, 11, 2), date(2026, 11, 9))
        == "_created:[2026-11-02 TO 2026-11-09}"
    )


def test_publication_year_query_includes_current_year():
    assert (
        catalogue.publication_year_query(5, date(2026, 11, 9))
        == "publication_year:[2022 TO 2026]"
    )


def test_pid_query():
    assert catalogue.pid_query(["a", "b"]) == '(pid:"a" OR pid:"b")'


def test_fetch_new_document_pids_follows_pagination_and_dedupes():
    next_items = "https://catalogue.library.cern/api/items/?page=2"
    session = FakeSession(
        {
            config.ITEMS_API: [page([item("d1"), item("d2")], next_url=next_items)],
            next_items: [page([item("d2"), {"metadata": {}}])],
            config.EITEMS_API: [page([item("d3"), item("d1")])],
        }
    )

    client = catalogue.LibraryCatalogue(token="secret", session=session)
    pids = client.fetch_new_document_pids(date(2026, 11, 2), date(2026, 11, 9))

    assert pids == ["d1", "d2", "d3"]
    first = session.calls[0]
    assert first["headers"] == {"Authorization": "Bearer secret"}
    assert first["params"]["q"] == (
        "_created:[2026-11-02 TO 2026-11-09} AND NOT restricted:true"
    )
    # The next link already carries the query string.
    assert session.calls[1] == {
        "url": next_items,
        "params": None,
        "headers": first["headers"],
    }


def test_fetch_new_document_pids_raises_on_http_error():
    session = FakeSession({config.ITEMS_API: [FakeResponse({}, status=401)]})
    with pytest.raises(RuntimeError):
        catalogue.LibraryCatalogue(
            token="bad", session=session
        ).fetch_new_document_pids(date(2026, 11, 2), date(2026, 11, 9))


def test_fetch_documents_chunks_pids_and_sets_page_size(monkeypatch):
    monkeypatch.setattr(config, "PID_CHUNK_SIZE", 2)
    subjects = [{"scheme": "UDC", "value": "53"}]
    session = FakeSession(
        {
            config.LITERATURE_API: [
                page([literature_hit("p1", subjects=subjects), literature_hit("p2")]),
                page([literature_hit("p3")]),
            ]
        }
    )

    client = catalogue.LibraryCatalogue(token="t", session=session)
    documents = client.fetch_documents(["p1", "p2", "p3"], years_window=None)

    assert [c["params"] for c in session.calls] == [
        {"q": '(pid:"p1" OR pid:"p2")', "size": 2},
        {"q": '(pid:"p3")', "size": 1},
    ]
    assert documents[0] == {
        "pid": "p1",
        "title": "A title",
        "document_type": "BOOK",
        "publication_year": "2024",
        "subjects": subjects,
    }
    assert [d["pid"] for d in documents] == ["p1", "p2", "p3"]


def test_fetch_documents_with_publication_year_filter():
    session = FakeSession({config.LITERATURE_API: [page([])]})

    catalogue.LibraryCatalogue(token="t", session=session).fetch_documents(
        ["p1"], years_window=5, today=date(2026, 11, 9)
    )

    assert session.calls[0]["params"]["q"] == (
        '(pid:"p1") AND publication_year:[2022 TO 2026]'
    )


def test_fetch_documents_without_pids_makes_no_request():
    session = FakeSession({})
    client = catalogue.LibraryCatalogue(token="t", session=session)
    assert client.fetch_documents([]) == []
    assert session.calls == []


def test_token_comes_from_airflow_connection(monkeypatch):
    class FakeConnection:
        password = "from-connection"

    monkeypatch.setattr(
        catalogue.BaseHook, "get_connection", lambda conn_id: FakeConnection()
    )

    client = catalogue.LibraryCatalogue(session=FakeSession({}))

    assert client.headers == {"Authorization": "Bearer from-connection"}
