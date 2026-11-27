"""Client for the REST API of the CERN Archive AtoM (arRestApiPlugin)."""

import logging
import mimetypes
import re
from urllib.parse import urlparse

import requests
from airflow.sdk import BaseHook

from digitization_workflows.repository_publish import config
from digitization_workflows.repository_publish.http import check_response, require_https

logger = logging.getLogger(__name__)

SLUG_REGEX = re.compile(config.SLUG_PATTERN)


ALREADY_LINKED_MESSAGE = "Already has a digital object"
# AtoM media types (terms of the "Media type" taxonomy), by MIME type.
MEDIA_TYPES = {"image": "Image", "text": "Text", "audio": "Audio", "video": "Video"}
TEXT_MIME_TYPES = {"application/pdf"}


def media_type(uri: str) -> str:
    """The AtoM media type of the file at `uri`, from its extension.

    The AtoM API stores "Other" when the request has no media type, and AtoM makes
    thumbnails only for images and texts.
    """
    mime_type, _ = mimetypes.guess_type(urlparse(uri).path.lower())
    if mime_type in TEXT_MIME_TYPES:
        return "Text"
    return MEDIA_TYPES.get((mime_type or "").split("/")[0], "Other")


class AtomError(Exception):
    """AtoM refused the digital object."""


class AtomAlreadyLinkedError(AtomError):
    """The description already has a digital object.

    The read API omits `digital_object` when the digital object fields are hidden in
    the AtoM settings, so this can happen even if the description had none.
    """


def validate_slug(slug: str) -> str:
    if not isinstance(slug, str) or not SLUG_REGEX.fullmatch(slug):
        raise ValueError(f"Invalid AtoM slug: {slug!r}")
    return slug


class AtomClient:
    """Reads descriptions and attaches digital objects.

    By default the URL is the host of the ``atom_conn`` connection, and the API key
    is its password.
    """

    def __init__(
        self,
        base_url: str | None = None,
        api_key: str | None = None,
        conn_id: str = config.ATOM_CONN_ID,
        session: requests.Session | None = None,
    ):
        if base_url is None or api_key is None:
            connection = BaseHook.get_connection(conn_id)
            base_url = connection.host if base_url is None else base_url
            api_key = connection.password if api_key is None else api_key
        if not base_url:
            raise ValueError(f"Set the AtoM URL in the host of '{conn_id}'.")
        if not api_key:
            raise ValueError(f"Set the AtoM API key in the password of '{conn_id}'.")

        self.base_url = require_https(base_url, conn_id)
        self.api_url = f"{self.base_url}/index.php/api"
        self.headers = {"REST-API-Key": api_key, "Accept": "application/json"}
        self.session = session or requests.Session()

    def description_url(self, slug: str) -> str:
        return f"{self.base_url}/index.php/{validate_slug(slug)}"

    def get_description(self, slug: str) -> dict | None:
        """The description, or None if there is no description with this slug."""
        url = f"{self.api_url}/informationobjects/{validate_slug(slug)}"
        response = self.session.request(
            "GET", url, headers=self.headers, timeout=config.REQUEST_TIMEOUT
        )
        if response.status_code == 404:
            return None
        return check_response(response).json()

    def attach_digital_object(self, slug: str, uri: str) -> dict:
        """Links the external file `uri` as the digital object of the description."""
        response = self.session.request(
            "POST",
            f"{self.api_url}/digitalobjects",
            headers=self.headers,
            json={
                "information_object_slug": validate_slug(slug),
                "uri": uri,
                "media_type": media_type(uri),
            },
            timeout=config.REQUEST_TIMEOUT,
        )
        if response.status_code == 403 and ALREADY_LINKED_MESSAGE in response.text:
            raise AtomAlreadyLinkedError(f"{slug}: {ALREADY_LINKED_MESSAGE}")
        if response.status_code in (400, 403):
            raise AtomError(
                f"AtoM refused the digital object of {slug}: {response.text[:500]}"
            )
        check_response(response)
        logger.info("%s: digital object linked to %s", slug, uri)
        return response.json()
