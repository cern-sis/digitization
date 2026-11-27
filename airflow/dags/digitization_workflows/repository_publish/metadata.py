"""Maps an AtoM description to the metadata of a repository.cern (InvenioRDM) record."""

import html
import re
from datetime import date as calendar_date

from digitization_workflows.repository_publish import config

CREATION_DATE_TYPE = "Creation"
YEAR_PATTERN = re.compile(r"\b(1[5-9]\d{2}|20\d{2})\b")
EDTF_DATE_PATTERN = re.compile(r"^\d{4}(-\d{2}(-\d{2})?)?$")


class MissingDateError(ValueError):
    """The AtoM description has no date to use as the publication date."""


def normalize_date(value: str | None) -> str | None:
    """`1954-03-00` -> `1954-03`: AtoM stores the unknown parts of a date as zeros."""
    if not value:
        return None
    kept = []
    for part in str(value).strip().split("-"):
        if not part.isdigit() or int(part) == 0:
            break
        kept.append(part)
    candidate = "-".join(kept)
    if not EDTF_DATE_PATTERN.fullmatch(candidate):
        return None
    return candidate if _is_valid_date(candidate) else candidate[:4]


def _is_valid_date(value: str) -> bool:
    """`1954-13` or `1954-02-31` are rejected by repository.cern: keep the year only."""
    year, month, day = ([int(p) for p in value.split("-")] + [1, 1])[:3]
    try:
        calendar_date(year, month, day)
    except ValueError:
        return False
    return True


def _interval(start: str, end: str) -> str:
    low, high = sorted((start, end))
    return low if low == high else f"{low}/{high}"


def _pick_date(dates: list[dict]) -> dict | None:
    creation = [d for d in dates if d.get("type") == CREATION_DATE_TYPE]
    return next(iter(creation or dates), None)


def publication_date(dates: list[dict] | None) -> str:
    """EDTF date or interval from the AtoM dates (the creation date first)."""
    date = _pick_date(dates or [])
    if date is None:
        raise MissingDateError("The AtoM description has no date.")

    start = normalize_date(date.get("start_date"))
    end = normalize_date(date.get("end_date"))
    if start or end:
        return _interval(start or end, end or start)

    years = YEAR_PATTERN.findall(date.get("date") or "")
    if years:
        return _interval(min(years), max(years))

    raise MissingDateError(f"No year in the AtoM date {date.get('date')!r}.")


def _creators(description: dict) -> list[dict]:
    names = [
        c.get("authorized_form_of_name") for c in description.get("creators") or []
    ]
    unique_names = list(dict.fromkeys(name for name in names if name))
    return [
        {"person_or_org": {"type": "organizational", "name": name}}
        for name in unique_names or [config.DEFAULT_CREATOR]
    ]


def _as_html(text: str) -> str:
    """The AtoM fields are plain text; the repository.cern description is HTML."""
    return html.escape(text.strip()).replace("\r\n", "\n").replace("\n", "<br>")


def _description(description: dict) -> str | None:
    parts = []
    if description.get("reference_code"):
        parts.append(f"Reference code: {description['reference_code']}")
    if description.get("scope_and_content"):
        parts.append(description["scope_and_content"])
    return "<br>".join(_as_html(part) for part in parts) or None


def atom_to_rdm(
    description: dict,
    slug: str,
    atom_url: str,
    resource_type: str = config.DEFAULT_RESOURCE_TYPE,
    publisher: str = config.DEFAULT_PUBLISHER,
) -> dict:
    """The record to create on repository.cern for the AtoM description.

    The AtoM URL is stored as a related identifier: it is how the DAG finds the
    record again on the next runs.
    """
    metadata = {
        "resource_type": {"id": resource_type},
        "title": description.get("title") or slug,
        "publication_date": publication_date(description.get("dates")),
        "creators": _creators(description),
        "publisher": publisher,
        "related_identifiers": [
            {
                "identifier": atom_url,
                "scheme": "url",
                "relation_type": {"id": "isdescribedby"},
                "resource_type": {"id": "other"},
            }
        ],
    }
    text = _description(description)
    if text:
        metadata["description"] = text

    return {
        "access": {"record": "public", "files": "public"},
        "files": {"enabled": True},
        "metadata": metadata,
    }


def merge_metadata(current: dict, mapped: dict) -> dict:
    """The metadata of a published record, refreshed from AtoM.

    The fields that `atom_to_rdm` maps are replaced. The other fields, for example
    subjects added by a curator, are kept. The related identifiers are merged, so
    that the AtoM URL is there and the other identifiers are kept.
    """
    merged = {**current, **mapped}
    current_related = current.get("related_identifiers", [])
    known = {item.get("identifier") for item in current_related}
    merged["related_identifiers"] = [*current_related] + [
        item
        for item in mapped.get("related_identifiers", [])
        if item.get("identifier") not in known
    ]
    return merged
