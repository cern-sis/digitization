"""Client for the CERN Library catalogue API."""

import logging
from datetime import date

import requests
from airflow.sdk import BaseHook, Variable

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

    By default it uses the same access as the BI DAGs: the URL is the host of the
    ``library_catalog_conn`` connection and the token is the ``CATALOG_API_TOKEN``
    Variable.
    """

    def __init__(
        self,
        base_url: str = None,
        token: str = None,
        conn_id: str = config.CATALOGUE_CONN_ID,
        session: requests.Session = None,
    ):
        if base_url is None:
            base_url = BaseHook.get_connection(conn_id).host
        if not base_url:
            raise ValueError(f"Set the catalogue URL in the host of '{conn_id}'.")
        if token is None:
            token = Variable.get(config.CATALOGUE_TOKEN_VARIABLE)

        self.base_url = base_url.rstrip("/")
        self.items_api = f"{self.base_url}/api/items/"
        self.eitems_api = f"{self.base_url}/api/eitems/"
        self.literature_api = f"{self.base_url}/api/literature/"
        self.headers = {"Authorization": f"Bearer {token}"}
        self.session = session or requests.Session()

        logger.info("Catalogue: %s", self.base_url)
        if self.base_url != config.PRODUCTION_CATALOGUE_URL:
            logger.warning(
                "%s is not the production catalogue (%s): the newsletter will not "
                "list the real new documents.",
                self.base_url,
                config.PRODUCTION_CATALOGUE_URL,
            )

    def document_url(self, pid: str) -> str:
        return f"{self.base_url}/literature/{pid}"

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

        pids = set()
        for url in (self.items_api, self.eitems_api):
            count = 0
            for hit in self._iter_hits(url, params, headers=self.headers):
                count += 1
                pid = hit.get("metadata", {}).get("document_pid")
                if pid:
                    pids.add(pid)
            logger.info("   -> %d records created in %s", count, url)

        logger.info("Found %d documents with new items/e-items.", len(pids))
        return sorted(pids)

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
            for hit in self._iter_hits(self.literature_api, params):
                document = summarize_document(hit)
                document["url"] = self.document_url(document["pid"])
                documents.append(document)

        logger.info(
            "Retrieved %d of %d documents from the public catalogue.",
            len(documents),
            len(pids),
        )
        return documents
