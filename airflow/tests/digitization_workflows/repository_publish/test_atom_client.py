import pytest

from digitization_workflows.repository_publish.atom_client import (
    AtomAlreadyLinkedError,
    AtomClient,
    AtomError,
    media_type,
)
from digitization_workflows.repository_publish.http import HttpError

BASE_URL = "https://archives.cern.ch"
API = f"{BASE_URL}/index.php/api"


def client_with(session):
    return AtomClient(base_url=BASE_URL + "/", api_key="key", session=session)


def test_description_url():
    assert (
        client_with(None).description_url("council-minutes-1954")
        == "https://archives.cern.ch/index.php/council-minutes-1954"
    )


def test_get_description_sends_api_key(fake_session, response):
    session = fake_session(
        {("GET", f"{API}/informationobjects/a-slug"): [response({"title": "T"})]}
    )

    assert client_with(session).get_description("a-slug") == {"title": "T"}
    assert session.calls[0]["headers"]["REST-API-Key"] == "key"
    assert session.calls[0]["headers"]["Accept"] == "application/json"


def test_get_description_returns_none_when_not_found(fake_session, response):
    session = fake_session(
        {("GET", f"{API}/informationobjects/missing"): [response({}, status=404)]}
    )

    assert client_with(session).get_description("missing") is None


def test_get_description_raises_on_server_error(fake_session, response):
    session = fake_session(
        {("GET", f"{API}/informationobjects/a-slug"): [response({}, status=500)]}
    )

    with pytest.raises(HttpError, match="HTTP 500"):
        client_with(session).get_description("a-slug")


def test_attach_digital_object_posts_slug_and_uri(fake_session, response):
    session = fake_session(
        {
            ("POST", f"{API}/digitalobjects"): [
                response({"id": 7, "slug": "do"}, status=201)
            ]
        }
    )

    result = client_with(session).attach_digital_object(
        "a-slug", "https://repository.cern/records/abc/files/a-slug.pdf"
    )

    assert result == {"id": 7, "slug": "do"}
    assert session.calls[0]["json"] == {
        "information_object_slug": "a-slug",
        "uri": "https://repository.cern/records/abc/files/a-slug.pdf",
        "media_type": "Text",
    }


def test_attach_digital_object_reports_atom_refusal(fake_session, response):
    session = fake_session(
        {
            ("POST", f"{API}/digitalobjects"): [
                response({"message": "Already has a digital object"}, status=403)
            ]
        }
    )

    with pytest.raises(AtomAlreadyLinkedError):
        client_with(session).attach_digital_object("a-slug", "https://x")


@pytest.mark.parametrize("slug", ["", "../admin", "a slug", "Upper", "a-slug\n"])
def test_invalid_slugs_are_rejected_before_any_request(slug):
    with pytest.raises(ValueError):
        client_with(None).get_description(slug)


def test_missing_settings_raise():
    with pytest.raises(ValueError):
        AtomClient(base_url="", api_key="key", session=object())
    with pytest.raises(ValueError):
        AtomClient(base_url=BASE_URL, api_key="", session=object())


def test_plain_http_is_refused():
    with pytest.raises(ValueError, match="https"):
        AtomClient(base_url="http://archives.cern.ch", api_key="k", session=object())


def test_attach_digital_object_other_refusal(fake_session, response):
    session = fake_session(
        {("POST", f"{API}/digitalobjects"): [response({"message": "bad"}, status=400)]}
    )

    with pytest.raises(AtomError, match="bad") as error:
        client_with(session).attach_digital_object("a-slug", "https://x")
    assert not isinstance(error.value, AtomAlreadyLinkedError)


@pytest.mark.parametrize(
    ("uri", "expected"),
    [
        ("https://x/records/r/files/CERN-ARCH-Test-002.jpg", "Image"),
        ("https://x/records/r/files/a.TIF", "Image"),
        ("https://x/records/r/files/a.pdf", "Text"),
        ("https://x/records/r/files/a.mp4", "Video"),
        ("https://x/records/r/files/a.mp3", "Audio"),
        ("https://x/records/r/files/a.zip", "Other"),
        ("https://x/records/r/files/no-extension", "Other"),
    ],
)
def test_media_type_follows_the_file_type(uri, expected):
    """Without it, the AtoM API stores "Other" and makes no thumbnail."""
    assert media_type(uri) == expected
