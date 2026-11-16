"""Client for the CERN Library catalogue API."""

import logging
from datetime import date

import requests
from airflow.sdk import BaseHook

from notifications import config

logger = logging.getLogger(__name__)

REQUEST_TIMEOUT = 60


def created_range_query(start: date, end: date) -> str:
    return f"_created:[{start.isoformat()} TO {end.isoformat()}}}"


def publication_year_query(years_window: int, today: date) -> str:
    return f"publication_year:[{today.year - years_window + 1} TO {today.year}]"


def pid_query(pids: list[str]) -> str:
    return "(" + " OR ".join(f'pid:"{pid}"' for pid in pids) + ")"


def summarize_document(hit: dict) -> dict:
    metadata = hit.get("metadata", {})
    return {
        "pid": metadata.get("pid") or hit.get("id"),
        "title": metadata.get("title", ""),
        "document_type": metadata.get("document_type"),
        "publication_year": metadata.get("publication_year"),
        "subjects": metadata.get("subjects", []),
    }


class LibraryCatalogue:
    """Reads new items/e-items and documents from the library catalogue.

    1. backoffice ``/api/items`` and ``/api/eitems`` (token required) give the
       ``document_pid`` of the items/e-items created in the period;
    2. the public ``/api/literature`` gives the metadata of those documents
       (restricted documents are not returned).
    """

    def __init__(
        self,
        token: str = None,
        conn_id: str = "library_catalogue_conn",
        session: requests.Session = None,
    ):
        if token is None:
            token = BaseHook.get_connection(conn_id).password
        self.headers = {"Authorization": f"Bearer {token}"}
        self.session = session or requests.Session()

    def _iter_hits(self, url: str, params: dict, headers: dict = None):
        next_url, next_params = url, params
        while next_url:
            response = self.session.get(
                next_url, params=next_params, headers=headers, timeout=REQUEST_TIMEOUT
            )
            response.raise_for_status()
            body = response.json()
            yield from body.get("hits", {}).get("hits", [])
            next_url, next_params = body.get("links", {}).get("next"), None

    def fetch_new_document_pids(self, start: date, end: date) -> list[str]:
        params = {
            "q": f"{created_range_query(start, end)} AND NOT restricted:true",
            "size": config.ITEMS_PAGE_SIZE,
        }

        pids = []
        for url in (config.ITEMS_API, config.EITEMS_API):
            count = 0
            for hit in self._iter_hits(url, params, headers=self.headers):
                count += 1
                pid = hit.get("metadata", {}).get("document_pid")
                if pid and pid not in pids:
                    pids.append(pid)
            logger.info(f"   -> {count} records created in {url}")

        logger.info(f"Found {len(pids)} documents with new items/e-items.")
        return pids

    def fetch_documents(
        self,
        pids: list[str],
        years_window: int = config.PUBLICATION_YEARS_WINDOW,
        today: date = None,
    ) -> list[dict]:
        """Public metadata of the given documents, queried in chunks of PIDs."""
        year_filter = (
            f" AND {publication_year_query(years_window, today or date.today())}"
            if years_window
            else ""
        )

        documents = []
        for i in range(0, len(pids), config.PID_CHUNK_SIZE):
            chunk = pids[i : i + config.PID_CHUNK_SIZE]
            params = {"q": pid_query(chunk) + year_filter, "size": len(chunk)}
            documents.extend(
                summarize_document(hit)
                for hit in self._iter_hits(config.LITERATURE_API, params)
            )

        logger.info(
            f"Retrieved {len(documents)} of {len(pids)} documents from the public catalogue."
        )
        return documents
