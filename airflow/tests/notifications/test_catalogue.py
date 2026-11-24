from datetime import date

import pytest

from notifications import catalogue, config

BASE_URL = "https://catalogue.library.cern"
ITEMS_API = f"{BASE_URL}/api/items/"
EITEMS_API = f"{BASE_URL}/api/eitems/"
LITERATURE_API = f"{BASE_URL}/api/literature/"


def client_with(session, token="t"):
    return catalogue.LibraryCatalogue(base_url=BASE_URL, token=token, session=session)


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
            ITEMS_API: [page([item("d1"), item("d2")], next_url=next_items)],
            next_items: [page([item("d2"), {"metadata": {}}])],
            EITEMS_API: [page([item("d3"), item("d1")])],
        }
    )

    client = client_with(session, token="secret")
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
    session = FakeSession({ITEMS_API: [FakeResponse({}, status=401)]})
    with pytest.raises(RuntimeError):
        client_with(session, token="bad").fetch_new_document_pids(
            date(2026, 11, 2), date(2026, 11, 9)
        )


def test_fetch_documents_chunks_pids_and_sets_page_size(monkeypatch):
    monkeypatch.setattr(config, "PID_CHUNK_SIZE", 2)
    subjects = [{"scheme": "UDC", "value": "53"}]
    session = FakeSession(
        {
            LITERATURE_API: [
                page([literature_hit("p1", subjects=subjects), literature_hit("p2")]),
                page([literature_hit("p3")]),
            ]
        }
    )

    client = client_with(session)
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
        "url": f"{BASE_URL}/literature/p1",
    }
    assert [d["pid"] for d in documents] == ["p1", "p2", "p3"]


def test_fetch_documents_with_publication_year_filter():
    session = FakeSession({LITERATURE_API: [page([])]})

    client_with(session).fetch_documents(
        ["p1"], years_window=5, today=date(2026, 11, 9)
    )

    assert session.calls[0]["params"]["q"] == (
        '(pid:"p1") AND publication_year:[2022 TO 2026]'
    )


def test_fetch_documents_without_pids_makes_no_request():
    session = FakeSession({})
    client = client_with(session)
    assert client.fetch_documents([]) == []
    assert session.calls == []


class FakeConnection:
    def __init__(self, host):
        self.host = host


def use_bi_access(monkeypatch, host, token="from-variable"):
    """Fakes the BI connection (URL in the host) and the token Variable."""
    asked = {}

    def get_connection(conn_id):
        asked["conn_id"] = conn_id
        return FakeConnection(host)

    def get_variable(key):
        asked["variable"] = key
        return token

    monkeypatch.setattr(catalogue.BaseHook, "get_connection", get_connection)
    monkeypatch.setattr(catalogue.Variable, "get", get_variable)
    return asked


def test_url_from_bi_connection_and_token_from_variable(monkeypatch, caplog):
    asked = use_bi_access(monkeypatch, "https://catalogue.library.cern/")

    client = catalogue.LibraryCatalogue(session=FakeSession({}))

    assert asked == {"conn_id": "library_catalog_conn", "variable": "CATALOG_API_TOKEN"}
    assert client.base_url == BASE_URL
    assert client.items_api == ITEMS_API
    assert client.document_url("abc") == f"{BASE_URL}/literature/abc"
    assert client.headers == {"Authorization": "Bearer from-variable"}
    assert "not the production catalogue" not in caplog.text


def test_warns_when_the_connection_is_not_the_production_catalogue(monkeypatch, caplog):
    use_bi_access(monkeypatch, "https://sandbox-cds-ils.web.cern.ch/")

    client = catalogue.LibraryCatalogue(session=FakeSession({}))

    assert client.items_api == "https://sandbox-cds-ils.web.cern.ch/api/items/"
    assert "not the production catalogue" in caplog.text


def test_connection_without_host_raises(monkeypatch):
    use_bi_access(monkeypatch, "")

    with pytest.raises(ValueError, match="library_catalog_conn"):
        catalogue.LibraryCatalogue(session=FakeSession({}))
