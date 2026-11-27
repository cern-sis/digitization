"""S3 file -> repository.cern record -> AtoM digital object, for one AtoM slug.

Every step checks what already exists first, so that a run continues where the
previous one stopped:

- the AtoM description already has a digital object: nothing to do;
- repository.cern already has a record with the AtoM URL: it is reused (a draft is
  completed, a record in review is accepted if `auto_accept` is on);
- otherwise a record is created, the file is uploaded and submitted to the community.

Once a file is linked, it is moved to the done folder (if one is set), so that the
source folder only contains the files that still need work.

With `refresh_metadata`, a published record that already existed gets the current
AtoM metadata, even if AtoM is already linked. With `replace_file`, it gets a new
version with the S3 file and the AtoM metadata (the files of a published record are
locked). AtoM is linked to the new version only if it has no digital object yet.
"""

import logging
import posixpath
import re
from collections import Counter
from dataclasses import dataclass
from typing import BinaryIO, Protocol

from digitization_workflows.repository_publish import config
from digitization_workflows.repository_publish.atom_client import AtomAlreadyLinkedError
from digitization_workflows.repository_publish.http import HttpError
from digitization_workflows.repository_publish.metadata import (
    MissingDateError,
    atom_to_rdm,
    merge_metadata,
)

logger = logging.getLogger(__name__)

DRY_RUN = "Dry Run"
DEPOSIT_ONLY = "Deposit only"
DEPOSIT_AND_LINK = "Deposit and link"
EXECUTION_MODES = [DRY_RUN, DEPOSIT_ONLY, DEPOSIT_AND_LINK]

SLUG_REGEX = re.compile(config.SLUG_PATTERN)
# Record statuses of InvenioRDM (computed from the review request of the draft).
DRAFT_STATUSES = {"draft", "draft_with_review"}  # not submitted yet
IN_REVIEW_STATUSES = {"in_review"}
DECLINED_STATUSES = {"declined", "expired"}
# Statuses of the files that were skipped before AtoM was read.
NOT_PROCESSED_STATUSES = {"invalid_slug", "duplicate_slug", "not_in_s3"}


class Status:
    LINKED = "linked"
    ALREADY_LINKED = "already_linked"
    PUBLISHED = "published"
    PENDING_REVIEW = "pending_review"
    REVIEW_DECLINED = "review_declined"
    WOULD_DEPOSIT = "would_deposit"
    DEFERRED = "deferred"
    NO_ATOM_DESCRIPTION = "no_atom_description"
    NO_DATE = "no_date"
    INVALID_SLUG = "invalid_slug"
    DUPLICATE_SLUG = "duplicate_slug"
    NOT_IN_S3 = "not_in_s3"
    ERROR = "error"


@dataclass(frozen=True)
class PublishSettings:
    mode: str
    community_id: str
    auto_accept: bool
    resource_type: str = config.DEFAULT_RESOURCE_TYPE
    # The S3 folder of the files, and where to move them once linked (None: keep).
    source_folder: str = ""
    done_folder: str | None = None
    # Update the published records that already existed with the AtoM metadata.
    refresh_metadata: bool = False
    # Publish a new version of them with the S3 file (and the AtoM metadata).
    replace_file: bool = False

    @property
    def is_dry_run(self) -> bool:
        return self.mode == DRY_RUN


class RecordUpdate:
    REFRESHED = "refreshed"
    WOULD_REFRESH = "would_refresh"
    NEW_VERSION = "new_version"
    WOULD_REPLACE_FILE = "would_replace_file"


class DepositError(Exception):
    """Creating a new record failed. It still counts against `max_deposits`."""


class FileSource(Protocol):
    def open_stream(self, key: str) -> tuple[BinaryIO, int]: ...

    def move_file(self, source_key: str, destination_key: str) -> None: ...


def result(
    slug: str,
    s3_key: str | None,
    status: str,
    record: dict | None = None,
    file_url: str | None = None,
    error: str | None = None,
    new_deposit: bool = False,
    moved_to: str | None = None,
    atom_url: str | None = None,
    record_update: str | None = None,
) -> dict:
    record_url = (record or {}).get("links", {}).get("self_html")
    return {
        "slug": slug,
        "s3_key": s3_key,
        "status": status,
        "record_url": record_url,
        "file_url": file_url,
        "error": error,
        "new_deposit": new_deposit,
        "moved_to": moved_to,
        "atom_url": atom_url,
        "record_update": record_update,
    }


def slug_from_key(key: str) -> str:
    """The file name without its extension, in lowercase like the AtoM slugs."""
    return posixpath.splitext(posixpath.basename(key))[0].lower()


def plan_items(keys: list[str], only_slugs: list[str] | None = None) -> dict:
    """Maps the S3 keys to AtoM slugs. Invalid and duplicate slugs are skipped."""
    by_slug: dict[str, list[str]] = {}
    for key in keys:
        by_slug.setdefault(slug_from_key(key), []).append(key)

    if only_slugs is not None:
        wanted = set(only_slugs)
        missing = [slug for slug in only_slugs if slug not in by_slug]
        by_slug = {slug: found for slug, found in by_slug.items() if slug in wanted}
    else:
        missing = []

    items, skipped = [], [result(slug, None, Status.NOT_IN_S3) for slug in missing]
    for slug, slug_keys in sorted(by_slug.items()):
        if not SLUG_REGEX.fullmatch(slug):
            skipped.extend(result(slug, key, Status.INVALID_SLUG) for key in slug_keys)
        elif len(slug_keys) > 1:
            skipped.extend(
                result(slug, key, Status.DUPLICATE_SLUG) for key in slug_keys
            )
        else:
            items.append({"slug": slug, "s3_key": slug_keys[0]})
    return {"items": items, "skipped": skipped}


def _upload_from_s3(
    repo, s3: FileSource, record_id: str, key: str, s3_key: str
) -> None:
    status = repo.draft_file_status(record_id, key)
    if status == "completed":
        return
    if status is not None:
        repo.delete_draft_file(record_id, key)

    stream, size = s3.open_stream(s3_key)
    try:
        repo.upload_file(record_id, key, stream, size)
    finally:
        stream.close()


def _accept(repo, request_id: str, record_id: str) -> bool:
    """Accepts the community submission. False if the token may not accept it."""
    try:
        repo.accept_request(request_id)
    except HttpError as error:
        if error.status_code != 403:
            raise
        logger.warning(
            "Record %s: auto_accept refused, the account of the token is not a "
            "curator of the community. The record waits for a curator.",
            record_id,
        )
        return False
    return True


def _submit(repo, record_id: str, settings: PublishSettings) -> dict | None:
    """Submits the draft. Returns the published record if it was accepted, else None."""
    request = repo.submit_to_community(record_id, settings.community_id)
    if not (settings.auto_accept and _accept(repo, request["id"], record_id)):
        return None
    return repo.get_record(record_id)


def _continue_record(repo, s3, record: dict, s3_key: str, settings) -> tuple[str, dict]:
    """Moves an unpublished record forward. Returns (status, latest record)."""
    status = record.get("status")
    if status in DECLINED_STATUSES:
        return Status.REVIEW_DECLINED, record
    if settings.is_dry_run:
        return Status.PENDING_REVIEW, record

    if status in DRAFT_STATUSES:
        _upload_from_s3(repo, s3, record["id"], posixpath.basename(s3_key), s3_key)
        published = _submit(repo, record["id"], settings)
        return (
            (Status.PUBLISHED, published)
            if published
            else (Status.PENDING_REVIEW, record)
        )

    if status in IN_REVIEW_STATUSES and settings.auto_accept:
        if _accept(repo, repo.review_request_id(record["id"]), record["id"]):
            return Status.PUBLISHED, repo.get_record(record["id"])
        return Status.PENDING_REVIEW, record

    if status not in IN_REVIEW_STATUSES:
        logger.warning("Record %s has the unknown status %r", record.get("id"), status)
    return Status.PENDING_REVIEW, record


def _deposit(repo, s3, record_json: dict, s3_key: str, settings) -> tuple[str, dict]:
    draft = repo.create_draft(record_json)
    _upload_from_s3(repo, s3, draft["id"], posixpath.basename(s3_key), s3_key)
    published = _submit(repo, draft["id"], settings)
    return (
        (Status.PUBLISHED, published) if published else (Status.PENDING_REVIEW, draft)
    )


def _published_file_url(repo, record: dict, key: str) -> str:
    keys = repo.record_file_keys(record["id"])
    if key not in keys:
        if len(keys) != 1:
            raise ValueError(f"Record {record['id']} has no file {key} (files: {keys})")
        key = keys[0]
    return repo.file_url(record, key)


def _refresh(repo, record: dict, description: dict, slug: str, atom_url: str, settings):
    """Updates a published record with the AtoM metadata. Returns (record, update)."""
    mapped = atom_to_rdm(description, slug, atom_url, settings.resource_type)
    if settings.is_dry_run:
        return record, RecordUpdate.WOULD_REFRESH
    record = repo.edit_published_record(
        record["id"], lambda current: merge_metadata(current, mapped["metadata"])
    )
    return record, RecordUpdate.REFRESHED


def _discard_draft(repo, record_id: str) -> None:
    try:
        repo.delete_draft(record_id)
    except Exception:  # the original error matters more: log this one
        logger.exception("Could not discard the draft %s: delete it by hand", record_id)


def _replace_file(repo, s3, record, description, slug, atom_url, s3_key, settings):
    """Publishes a new version of the record with the S3 file and the AtoM metadata.

    Returns (latest record, update). An identical file creates no new version (every
    version stays public): only the metadata is refreshed.
    """
    if settings.is_dry_run:
        return record, RecordUpdate.WOULD_REPLACE_FILE
    mapped = atom_to_rdm(description, slug, atom_url, settings.resource_type)
    key = posixpath.basename(s3_key)
    draft = repo.new_version(
        record["id"], lambda current: merge_metadata(current, mapped["metadata"])
    )
    try:
        _upload_from_s3(repo, s3, draft["id"], key, s3_key)
        uploaded = repo.file_checksums(draft["id"], draft=True).get(key)
        unchanged = uploaded in repo.file_checksums(record["id"]).values()
    except Exception:
        _discard_draft(repo, draft["id"])
        raise
    if unchanged:
        _discard_draft(repo, draft["id"])
        logger.info("%s: this file is already published, no new version.", slug)
        return _refresh(repo, record, description, slug, atom_url, settings)

    published = repo.publish_draft(draft["id"])
    logger.info("%s: new version %s published", slug, published.get("id"))
    return published, RecordUpdate.NEW_VERSION


def _update_record(repo, s3, record, description, slug, atom_url, s3_key, settings):
    """Applies `replace_file` or `refresh_metadata` to a published record."""
    if settings.replace_file:
        return _replace_file(
            repo, s3, record, description, slug, atom_url, s3_key, settings
        )
    if settings.refresh_metadata:
        return _refresh(repo, record, description, slug, atom_url, settings)
    return record, None


def _update_linked(slug, s3_key, s3, repo, description, atom_url, settings) -> dict:
    """AtoM is already linked: only the record can be updated, not the AtoM link."""
    record = repo.find_record(atom_url)
    if not (record and record.get("is_published")):
        logger.warning("%s: no published record to update.", slug)
        return result(slug, s3_key, Status.ALREADY_LINKED, record)
    record, update = _update_record(
        repo, s3, record, description, slug, atom_url, s3_key, settings
    )
    if update == RecordUpdate.NEW_VERSION:
        logger.warning(
            "%s: AtoM still links the file of the previous version. Delete its "
            "digital object in AtoM, then run again with reprocess_migrated and "
            "Deposit and link.",
            slug,
        )
    return result(slug, s3_key, Status.ALREADY_LINKED, record, record_update=update)


def _publish(slug, s3_key, s3, atom, repo, settings, can_deposit) -> dict:
    description = atom.get_description(slug)
    if description is None:
        return result(slug, s3_key, Status.NO_ATOM_DESCRIPTION)

    atom_url = atom.description_url(slug)
    if description.get("digital_object"):
        if settings.refresh_metadata or settings.replace_file:
            return _update_linked(
                slug, s3_key, s3, repo, description, atom_url, settings
            )
        return result(slug, s3_key, Status.ALREADY_LINKED)

    record = repo.find_record(atom_url)
    new_deposit = record is None
    update = None

    if record is None:
        record_json = atom_to_rdm(description, slug, atom_url, settings.resource_type)
        if settings.is_dry_run:
            return result(slug, s3_key, Status.WOULD_DEPOSIT)
        if not can_deposit:
            return result(slug, s3_key, Status.DEFERRED)
        try:
            status, record = _deposit(repo, s3, record_json, s3_key, settings)
        except Exception as error:
            raise DepositError(str(error)) from error
    elif record.get("is_published"):
        status = Status.PUBLISHED
        record, update = _update_record(
            repo, s3, record, description, slug, atom_url, s3_key, settings
        )
    else:
        status, record = _continue_record(repo, s3, record, s3_key, settings)

    if status != Status.PUBLISHED:
        return result(slug, s3_key, status, record, new_deposit=new_deposit)

    file_url = _published_file_url(repo, record, posixpath.basename(s3_key))
    done = {"new_deposit": new_deposit, "record_update": update}
    if settings.mode != DEPOSIT_AND_LINK:
        return result(slug, s3_key, Status.PUBLISHED, record, file_url, **done)

    try:
        atom.attach_digital_object(slug, file_url)
    except AtomAlreadyLinkedError:
        logger.warning(
            "%s: AtoM already has a digital object, %s is not linked.", slug, file_url
        )
        return result(slug, s3_key, Status.ALREADY_LINKED, record, file_url, **done)
    return result(slug, s3_key, Status.LINKED, record, file_url, **done)


def publish_slug(
    slug: str,
    s3_key: str,
    s3: FileSource,
    atom,
    repo,
    settings: PublishSettings,
    can_deposit: bool = True,
) -> dict:
    """Publishes one S3 file and links it to its AtoM description. Never raises:
    an error is returned in the result, so that it does not stop the other slugs."""
    row = _publish_or_report(slug, s3_key, s3, atom, repo, settings, can_deposit)
    if row["status"] == Status.NO_ATOM_DESCRIPTION:
        return row
    return {**row, "atom_url": atom.description_url(slug)}


def _publish_or_report(slug, s3_key, s3, atom, repo, settings, can_deposit) -> dict:
    try:
        return _publish(slug, s3_key, s3, atom, repo, settings, can_deposit)
    except MissingDateError as error:
        logger.warning("%s: %s", slug, error)
        return result(slug, s3_key, Status.NO_DATE, error=str(error))
    except DepositError as error:
        logger.exception("%s: the new record failed", slug)
        return result(slug, s3_key, Status.ERROR, error=str(error), new_deposit=True)
    except Exception as error:  # reported per slug, see the docstring
        logger.exception("%s: failed", slug)
        return result(slug, s3_key, Status.ERROR, error=str(error))


def _move_to_done_folder(row: dict, s3: FileSource, settings: PublishSettings) -> dict:
    """Moves the S3 file of a linked row to the done folder. Returns the new row.

    A failed move does not undo the link: the row stays `linked`, with the error.
    """
    if row["status"] != Status.LINKED or not settings.done_folder:
        return row

    source = row["s3_key"]
    if source.startswith(settings.done_folder):  # reprocessed from the done folder
        return row
    if source.startswith(settings.source_folder):
        relative = source[len(settings.source_folder) :]
    else:
        relative = posixpath.basename(source)
    destination = settings.done_folder + relative

    try:
        s3.move_file(source, destination)
    except Exception as error:  # the link is done: report, do not fail the slug
        logger.exception("%s: linked, but not moved to %s", row["slug"], destination)
        return {**row, "error": f"Linked, but not moved to {destination}: {error}"}
    logger.info("%s: moved to %s", row["slug"], destination)
    return {**row, "moved_to": destination}


def publish_all(
    items, s3, atom, repo, settings: PublishSettings, max_deposits: int
) -> list[dict]:
    """Publishes the items in order.

    At most `max_deposits` new records are attempted: the next items that need a new
    record are `deferred` to the next run. After `MAX_CONSECUTIVE_ERRORS` errors in a
    row, the other items are `deferred` too. Linked files are moved to the done
    folder right away, so that a crash later in the run does not leave them behind.
    """
    rows, deposits, errors_in_row = [], 0, 0
    for index, item in enumerate(items):
        if errors_in_row >= config.MAX_CONSECUTIVE_ERRORS:
            message = f"Stopped after {errors_in_row} errors in a row."
            logger.error("%s %d files deferred.", message, len(items) - index)
            rows.extend(
                result(i["slug"], i["s3_key"], Status.DEFERRED, error=message)
                for i in items[index:]
            )
            break
        row = publish_slug(
            item["slug"],
            item["s3_key"],
            s3,
            atom,
            repo,
            settings,
            can_deposit=deposits < max_deposits,
        )
        row = _move_to_done_folder(row, s3, settings)
        deposits += int(row["new_deposit"])
        errors_in_row = errors_in_row + 1 if row["status"] == Status.ERROR else 0
        logger.info("%s: %s", row["slug"], row["status"])
        rows.append(row)
    return rows


LINK_LABELS = (
    ("record_url", "CDS record"),
    ("file_url", "CDS file"),
    ("atom_url", "AtoM"),
)


def link_lines(rows: list[dict]) -> list[str]:
    """One block per file: its status, its CDS and AtoM links, and where it is in S3."""
    blocks = []
    for row in rows:
        status = row["status"]
        if row.get("record_update"):
            status += f", record {row['record_update']}"
        lines = [f"{row['slug']} [{status}]"]
        lines.extend(
            f"  {label + ':':<11} {row[field]}"
            for field, label in LINK_LABELS
            if row.get(field)
        )
        s3_key = row.get("moved_to") or row.get("s3_key")
        if s3_key:
            lines.append(f"  {'S3:':<11} {s3_key}")
        blocks.append("\n".join(lines))
    return blocks


def summarize(rows: list[dict]) -> dict[str, int]:
    """The number of files per status, then per metadata update (if any)."""
    summary = Counter(row["status"] for row in rows)
    summary.update(
        f"record_{row['record_update']}" for row in rows if row.get("record_update")
    )
    return dict(summary)


def run_problems(rows: list[dict]) -> list[str]:
    """The reasons to mark the run as failed, so that they are not missed."""
    problems = []
    failed = sum(row["status"] == Status.ERROR for row in rows)
    if failed:
        problems.append(f"{failed} files failed. See the rows with status 'error'.")

    not_moved = sum(
        row["status"] == Status.LINKED and bool(row.get("error")) for row in rows
    )
    if not_moved:
        problems.append(
            f"{not_moved} linked files were not moved to the done folder: move them "
            "by hand (the next runs report them as 'already_linked')."
        )

    processed = [row for row in rows if row["status"] not in NOT_PROCESSED_STATUSES]
    if processed and all(r["status"] == Status.NO_ATOM_DESCRIPTION for r in processed):
        problems.append(
            "No file has an AtoM description: check the host and the API key of "
            f"'{config.ATOM_CONN_ID}', and the S3 file names."
        )
    return problems
