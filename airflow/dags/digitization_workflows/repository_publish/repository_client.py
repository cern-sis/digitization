"""Client for the REST API of repository.cern (InvenioRDM)."""

import logging
from collections.abc import Callable
from typing import BinaryIO
from urllib.parse import quote

import requests
from airflow.sdk import BaseHook

from digitization_workflows.repository_publish import config
from digitization_workflows.repository_publish.http import check_response, require_https

logger = logging.getLogger(__name__)

# The parts of a draft that are sent back when its metadata is edited.
EDITABLE_PARTS = ("access", "files", "pids", "custom_fields")


class SizedStream:
    """A file-like stream with a length, so that `requests` sends a Content-Length
    instead of a chunked upload (an S3 body has no length of its own)."""

    def __init__(self, stream: BinaryIO, size: int):
        self.stream = stream
        self.size = size

    def __len__(self) -> int:
        return self.size

    def read(self, size: int = -1) -> bytes:
        return self.stream.read(size)


def _key(key: str) -> str:
    return quote(key, safe="")


class RepositoryClient:
    """Creates, uploads, submits and finds records.

    By default the URL is the host of the ``repository_cern_conn`` connection, and
    the personal token (scope ``deposit:write``) is its password.
    """

    def __init__(
        self,
        base_url: str | None = None,
        token: str | None = None,
        conn_id: str = config.REPOSITORY_CONN_ID,
        session: requests.Session | None = None,
    ):
        if base_url is None or token is None:
            connection = BaseHook.get_connection(conn_id)
            base_url = connection.host if base_url is None else base_url
            token = connection.password if token is None else token
        if not base_url:
            raise ValueError(f"Set the repository URL in the host of '{conn_id}'.")
        if not token:
            raise ValueError(
                f"Set the repository token in the password of '{conn_id}'."
            )

        self.api_url = f"{require_https(base_url, conn_id)}/api"
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        }
        self.session = session or requests.Session()

    def _request(self, method: str, path: str, headers: dict | None = None, **kwargs):
        response = self.session.request(
            method,
            f"{self.api_url}{path}",
            headers={**self.headers, **(headers or {})},
            timeout=kwargs.pop("timeout", config.REQUEST_TIMEOUT),
            **kwargs,
        )
        return check_response(response)

    def _json(self, method: str, path: str, **kwargs) -> dict:
        return self._request(method, path, **kwargs).json()

    def community_id(self, slug: str) -> str:
        return self._json("GET", f"/communities/{_key(slug)}")["id"]

    def find_record(self, *identifiers: str) -> dict | None:
        """The record with one of these related identifiers: published first, then
        our drafts.

        The search index is updated a few seconds after a write: run the DAG again
        only after the previous run has finished.
        """
        if not identifiers:
            raise ValueError("find_record needs an identifier.")
        for identifier in identifiers:
            if '"' in identifier or "\\" in identifier:
                raise ValueError(f"Invalid identifier: {identifier!r}")
        quoted = " OR ".join(f'"{identifier}"' for identifier in identifiers)
        params = {"q": f"metadata.related_identifiers.identifier:({quoted})", "size": 2}
        for path in ("/records", "/user/records"):
            found = self._json("GET", path, params=params)["hits"]["hits"]
            if len(found) > 1:
                logger.warning(
                    "Several records for %s: %s. Using the first one, remove the others.",
                    " or ".join(identifiers),
                    [record.get("id") for record in found],
                )
            if found:
                return found[0]
        return None

    def _update_draft(self, draft: dict, update: Callable[[dict], dict]) -> dict:
        """Saves the draft with `update(its metadata)`. Returns the saved draft."""
        body = {key: draft[key] for key in EDITABLE_PARTS if key in draft}
        body["metadata"] = update(draft.get("metadata", {}))
        return self._json("PUT", f"/records/{draft['id']}/draft", json=body)

    def edit_published_record(
        self, record_id: str, update: Callable[[dict], dict]
    ) -> dict:
        """Publishes a new revision of the record with `update(current metadata)`.

        Same record and files: only the metadata changes. A published record of a
        community is edited without a new review.
        """
        draft = self._json("POST", f"/records/{record_id}/draft")
        self._update_draft({**draft, "id": record_id}, update)
        record = self.publish_draft(record_id)
        logger.info("Record %s: metadata updated", record_id)
        return record

    def new_version(self, record_id: str, update: Callable[[dict], dict]) -> dict:
        """A draft of a new version of the record, with `update(metadata)` and no
        file yet. The new version has its own id; the record stays published."""
        draft = self._json("POST", f"/records/{record_id}/versions")
        logger.info("Record %s: new version draft %s", record_id, draft.get("id"))
        return self._update_draft(draft, update)

    def publish_draft(self, record_id: str) -> dict:
        return self._json("POST", f"/records/{record_id}/draft/actions/publish")

    def delete_draft(self, record_id: str) -> None:
        """Discards the draft (of a new record, an edit or a new version)."""
        self._request("DELETE", f"/records/{record_id}/draft")

    def file_checksums(self, record_id: str, draft: bool = False) -> dict[str, str]:
        """{file key: checksum} of the published record, or of its draft."""
        path = (
            f"/records/{record_id}/draft/files"
            if draft
            else f"/records/{record_id}/files"
        )
        entries = self._json("GET", path).get("entries", [])
        return {entry["key"]: entry.get("checksum") for entry in entries}

    def create_draft(self, record: dict) -> dict:
        draft = self._json("POST", "/records", json=record)
        logger.info("Draft %s created", draft.get("id"))
        return draft

    def upload_file(
        self, record_id: str, key: str, stream: BinaryIO, size: int
    ) -> None:
        files = f"/records/{record_id}/draft/files"
        self._request("POST", files, json=[{"key": key}])
        self._request(
            "PUT",
            f"{files}/{_key(key)}/content",
            data=SizedStream(stream, size),
            headers={"Content-Type": "application/octet-stream"},
            timeout=config.UPLOAD_TIMEOUT,
        )
        committed = self._json("POST", f"{files}/{_key(key)}/commit")
        if committed.get("size") not in (None, size):
            raise ValueError(
                f"Draft {record_id}: {key} has {committed['size']} bytes, expected {size}"
            )
        logger.info("Draft %s: %s uploaded (%d bytes)", record_id, key, size)

    def draft_file_status(self, record_id: str, key: str) -> str | None:
        """`completed`, `pending`, ... or None if the draft has no file with this key."""
        entries = self._json("GET", f"/records/{record_id}/draft/files").get(
            "entries", []
        )
        return next((e.get("status") for e in entries if e.get("key") == key), None)

    def delete_draft_file(self, record_id: str, key: str) -> None:
        self._request("DELETE", f"/records/{record_id}/draft/files/{_key(key)}")

    def submit_to_community(self, record_id: str, community_id: str) -> dict:
        """Submits the draft to the community. Returns the review request."""
        draft = f"/records/{record_id}/draft"
        self._request(
            "PUT",
            f"{draft}/review",
            json={
                "receiver": {"community": community_id},
                "type": "community-submission",
            },
        )
        request = self._json(
            "POST",
            f"{draft}/actions/submit-review",
            json={"payload": {"content": config.SUBMISSION_MESSAGE, "format": "html"}},
        )
        logger.info(
            "Draft %s submitted for review (request %s)", record_id, request.get("id")
        )
        return request

    def review_request_id(self, record_id: str) -> str:
        """ID of the review request of a draft (e.g. a draft in review)."""
        return self._json("GET", f"/records/{record_id}/draft/review")["id"]

    def accept_request(self, request_id: str) -> None:
        self._request(
            "POST",
            f"/requests/{request_id}/actions/accept",
            json={"payload": {"content": config.ACCEPT_MESSAGE, "format": "html"}},
        )
        logger.info("Request %s accepted", request_id)

    def get_record(self, record_id: str) -> dict:
        return self._json("GET", f"/records/{record_id}")

    def record_file_keys(self, record_id: str) -> list[str]:
        entries = self._json("GET", f"/records/{record_id}/files").get("entries", [])
        return [entry["key"] for entry in entries]

    @staticmethod
    def file_url(record: dict, key: str) -> str:
        """Public link of a file of a published record."""
        return f"{record['links']['self_html']}/files/{_key(key)}"
