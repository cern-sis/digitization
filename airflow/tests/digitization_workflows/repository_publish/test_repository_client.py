import pytest
import requests

from digitization_workflows.repository_publish.http import HttpError
from digitization_workflows.repository_publish.repository_client import (
    RepositoryClient,
    SizedStream,
)

BASE_URL = "https://repository.cern"
API = f"{BASE_URL}/api"
DRAFT = f"{API}/records/abc-123/draft"
ATOM_URL = "https://archives.cern.ch/index.php/a-slug"
QUERY = f'metadata.related_identifiers.identifier:("{ATOM_URL}")'


def client_with(session):
    return RepositoryClient(base_url=BASE_URL + "/", token="tok", session=session)


class FakeStream:
    def __init__(self, data: bytes):
        self.data = data

    def read(self, size=-1):
        size = len(self.data) if size < 0 else size
        chunk, self.data = self.data[:size], self.data[size:]
        return chunk


def hits(*records):
    return {"hits": {"hits": list(records), "total": len(records)}}


def test_requests_send_the_token(fake_session, response):
    session = fake_session(
        {("GET", f"{API}/communities/my-community"): [response({"id": "uuid-1"})]}
    )

    assert client_with(session).community_id("my-community") == "uuid-1"
    assert session.calls[0]["headers"]["Authorization"] == "Bearer tok"


def test_find_record_prefers_published_records(fake_session, response):
    published = {"id": "pub-1", "is_published": True}
    session = fake_session({("GET", f"{API}/records"): [response(hits(published))]})

    assert client_with(session).find_record(ATOM_URL) == published
    assert session.calls[0]["params"] == {"q": QUERY, "size": 2}


def test_find_record_falls_back_to_own_drafts(fake_session, response):
    draft = {"id": "draft-1", "is_published": False, "status": "in_review"}
    session = fake_session(
        {
            ("GET", f"{API}/records"): [response(hits())],
            ("GET", f"{API}/user/records"): [response(hits(draft))],
        }
    )

    assert client_with(session).find_record(ATOM_URL) == draft


def test_find_record_returns_none(fake_session, response):
    session = fake_session(
        {
            ("GET", f"{API}/records"): [response(hits())],
            ("GET", f"{API}/user/records"): [response(hits())],
        }
    )

    assert client_with(session).find_record(ATOM_URL) is None


def test_find_record_matches_any_of_the_identifiers(fake_session, response):
    session = fake_session({("GET", f"{API}/records"): [response(hits({"id": "r"}))]})

    client_with(session).find_record("CERN-ARCH-A-1", ATOM_URL)

    assert session.calls[0]["params"]["q"] == (
        f'metadata.related_identifiers.identifier:("CERN-ARCH-A-1" OR "{ATOM_URL}")'
    )


def test_find_record_needs_an_identifier():
    with pytest.raises(ValueError):
        client_with(None).find_record()


def test_find_record_rejects_quotes_in_identifier():
    with pytest.raises(ValueError):
        client_with(None).find_record('x" OR *')


def test_create_draft(fake_session, response):
    session = fake_session(
        {("POST", f"{API}/records"): [response({"id": "abc-123"}, status=201)]}
    )

    assert client_with(session).create_draft({"metadata": {}}) == {"id": "abc-123"}
    assert session.calls[0]["json"] == {"metadata": {}}


def test_upload_file_declares_uploads_and_commits(fake_session, response):
    key = "a slug.pdf"
    encoded = "a%20slug.pdf"
    session = fake_session(
        {
            ("POST", f"{DRAFT}/files"): [response({}, status=201)],
            ("PUT", f"{DRAFT}/files/{encoded}/content"): [response({})],
            ("POST", f"{DRAFT}/files/{encoded}/commit"): [response({})],
        }
    )

    client_with(session).upload_file("abc-123", key, FakeStream(b"%PDF-1.4"), 8)

    declare, upload, commit = session.calls
    assert declare["json"] == [{"key": key}]
    assert upload["headers"]["Content-Type"] == "application/octet-stream"
    assert len(upload["data"]) == 8
    assert upload["data"].read(-1) == b"%PDF-1.4"
    assert commit["method"] == "POST"


def test_draft_file_status(fake_session, response):
    entries = {"entries": [{"key": "a.pdf", "status": "pending"}]}
    session = fake_session(
        {("GET", f"{DRAFT}/files"): [response(entries), response(entries)]}
    )
    client = client_with(session)

    assert client.draft_file_status("abc-123", "a.pdf") == "pending"
    assert client.draft_file_status("abc-123", "b.pdf") is None


def test_delete_draft_file(fake_session, response):
    session = fake_session(
        {("DELETE", f"{DRAFT}/files/a.pdf"): [response({}, status=204)]}
    )

    client_with(session).delete_draft_file("abc-123", "a.pdf")

    assert session.calls[0]["method"] == "DELETE"


def test_submit_to_community(fake_session, response):
    session = fake_session(
        {
            ("PUT", f"{DRAFT}/review"): [response({})],
            ("POST", f"{DRAFT}/actions/submit-review"): [response({"id": "req-1"})],
        }
    )

    request = client_with(session).submit_to_community("abc-123", "uuid-1")

    assert request == {"id": "req-1"}
    assert session.calls[0]["json"] == {
        "receiver": {"community": "uuid-1"},
        "type": "community-submission",
    }
    assert session.calls[1]["json"]["payload"]["format"] == "html"


def test_accept_request(fake_session, response):
    session = fake_session(
        {
            ("POST", f"{API}/requests/req-1/actions/accept"): [
                response({"status": "accepted"})
            ]
        }
    )

    client_with(session).accept_request("req-1")

    assert session.calls[0]["json"]["payload"]["format"] == "html"


def test_get_record_and_file_keys(fake_session, response):
    session = fake_session(
        {
            ("GET", f"{API}/records/abc-123"): [response({"id": "abc-123"})],
            ("GET", f"{API}/records/abc-123/files"): [
                response({"entries": [{"key": "a.pdf"}, {"key": "b.pdf"}]})
            ],
        }
    )
    client = client_with(session)

    assert client.get_record("abc-123") == {"id": "abc-123"}
    assert client.record_file_keys("abc-123") == ["a.pdf", "b.pdf"]


def test_file_url():
    record = {"links": {"self_html": "https://repository.cern/records/abc-123"}}

    assert (
        RepositoryClient.file_url(record, "a slug.pdf")
        == "https://repository.cern/records/abc-123/files/a%20slug.pdf"
    )


def test_find_record_warns_about_duplicates(fake_session, response, caplog):
    session = fake_session(
        {("GET", f"{API}/records"): [response(hits({"id": "r1"}, {"id": "r2"}))]}
    )

    assert client_with(session).find_record(ATOM_URL) == {"id": "r1"}
    assert "Several records" in caplog.text


def test_errors_include_the_response_body(fake_session, response):
    session = fake_session(
        {
            ("POST", f"{API}/records"): [
                response({"errors": [{"field": "metadata.title"}]}, status=400)
            ]
        }
    )

    with pytest.raises(HttpError, match="metadata.title") as error:
        client_with(session).create_draft({})
    assert error.value.status_code == 400


def test_upload_checks_the_committed_size(fake_session, response):
    session = fake_session(
        {
            ("POST", f"{DRAFT}/files"): [response({}, status=201)],
            ("PUT", f"{DRAFT}/files/a.pdf/content"): [response({})],
            ("POST", f"{DRAFT}/files/a.pdf/commit"): [response({"size": 3})],
        }
    )

    with pytest.raises(ValueError, match="3 bytes, expected 8"):
        client_with(session).upload_file("abc-123", "a.pdf", FakeStream(b"x" * 8), 8)


def test_review_request_id(fake_session, response):
    session = fake_session({("GET", f"{DRAFT}/review"): [response({"id": "req-9"})]})

    assert client_with(session).review_request_id("abc-123") == "req-9"


def test_plain_http_is_refused():
    with pytest.raises(ValueError, match="https"):
        RepositoryClient(base_url="http://repository.cern", token="t", session=object())


def test_sized_stream_is_sent_with_a_content_length():
    prepared = requests.Request(
        "PUT", "https://repository.cern/x", data=SizedStream(FakeStream(b"%PDF-1"), 6)
    ).prepare()

    assert prepared.headers["Content-Length"] == "6"
    assert "Transfer-Encoding" not in prepared.headers
    assert prepared.body.read(-1) == b"%PDF-1"


def test_edit_published_record(fake_session, response):
    draft = {
        "id": "abc-123",
        "access": {"record": "public", "files": "public"},
        "files": {"enabled": True},
        "pids": {"doi": {"identifier": "10.1/x"}},
        "metadata": {"title": "Old"},
        "links": {"self": "x"},
    }
    session = fake_session(
        {
            ("POST", DRAFT): [response(draft, status=201)],
            ("PUT", DRAFT): [response({})],
            ("POST", f"{DRAFT}/actions/publish"): [
                response({"id": "abc-123"}, status=202)
            ],
        }
    )

    published = client_with(session).edit_published_record(
        "abc-123", lambda current: {**current, "title": "New"}
    )

    assert published == {"id": "abc-123"}
    edit, update, publish = session.calls
    assert (edit["method"], update["method"], publish["method"]) == (
        "POST",
        "PUT",
        "POST",
    )
    assert update["json"] == {
        "access": {"record": "public", "files": "public"},
        "files": {"enabled": True},
        "pids": {"doi": {"identifier": "10.1/x"}},
        "metadata": {"title": "New"},
    }


NEW_DRAFT = f"{API}/records/new-1/draft"


def test_new_version_creates_a_draft_with_the_metadata(fake_session, response):
    version = {
        "id": "new-1",
        "access": {"record": "public", "files": "public"},
        "files": {"enabled": True},
        "metadata": {"title": "Old", "subjects": [{"subject": "S"}]},
    }
    session = fake_session(
        {
            ("POST", f"{API}/records/abc-123/versions"): [
                response(version, status=201)
            ],
            ("PUT", NEW_DRAFT): [response({"id": "new-1"})],
        }
    )

    draft = client_with(session).new_version(
        "abc-123", lambda current: {**current, "title": "New"}
    )

    assert draft == {"id": "new-1"}
    assert session.calls[1]["json"] == {
        "access": {"record": "public", "files": "public"},
        "files": {"enabled": True},
        "metadata": {"title": "New", "subjects": [{"subject": "S"}]},
    }


def test_publish_and_delete_draft(fake_session, response):
    session = fake_session(
        {
            ("POST", f"{NEW_DRAFT}/actions/publish"): [
                response({"id": "new-1"}, status=202)
            ],
            ("DELETE", NEW_DRAFT): [response({}, status=204)],
        }
    )
    client = client_with(session)

    assert client.publish_draft("new-1") == {"id": "new-1"}
    client.delete_draft("new-1")

    assert [c["method"] for c in session.calls] == ["POST", "DELETE"]


def test_file_checksums(fake_session, response):
    entries = {"entries": [{"key": "a.pdf", "checksum": "md5:1"}]}
    session = fake_session(
        {
            ("GET", f"{API}/records/abc-123/files"): [response(entries)],
            ("GET", f"{DRAFT}/files"): [response(entries)],
        }
    )
    client = client_with(session)

    assert client.file_checksums("abc-123") == {"a.pdf": "md5:1"}
    assert client.file_checksums("abc-123", draft=True) == {"a.pdf": "md5:1"}
    assert session.calls[1]["url"].endswith("/records/abc-123/draft/files")
