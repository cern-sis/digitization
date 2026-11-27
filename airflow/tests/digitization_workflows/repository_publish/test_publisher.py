import pytest

from digitization_workflows.repository_publish import publisher
from digitization_workflows.repository_publish.atom_client import AtomAlreadyLinkedError
from digitization_workflows.repository_publish.http import HttpError
from digitization_workflows.repository_publish.publisher import (
    DEPOSIT_AND_LINK,
    DEPOSIT_ONLY,
    DRY_RUN,
    PublishSettings,
    Status,
)

ATOM_BASE = "https://archives.cern.ch/index.php"
RECORD_HTML = "https://repository.cern/records/rec-1"
FILE_URL = f"{RECORD_HTML}/files/a-slug.pdf"

DESCRIPTION = {"title": "T", "dates": [{"date": "1954"}]}


class FakeAtom:
    def __init__(self, descriptions):
        self.descriptions = descriptions
        self.attached = []

    def description_url(self, slug):
        return f"{ATOM_BASE}/{slug}"

    def get_description(self, slug):
        return self.descriptions.get(slug)

    def attach_digital_object(self, slug, uri):
        self.attached.append((slug, uri))
        return {"id": 1}


RECORD2_HTML = "https://repository.cern/records/rec-2"


class FakeRepository:
    def __init__(
        self,
        existing=None,
        file_status=None,
        file_keys=("a-slug.pdf",),
        uploaded_checksum="md5:new",
        fail_upload=False,
        accept_error=None,
    ):
        self.existing = existing or {}
        self.file_status = file_status
        self.file_keys = list(file_keys)
        self.uploaded_checksum = uploaded_checksum
        self.fail_upload = fail_upload
        self.accept_error = accept_error
        self.writes = []

    def find_record(self, *identifiers):
        self.searched = identifiers
        return next((self.existing[i] for i in identifiers if i in self.existing), None)

    def create_draft(self, record):
        self.writes.append(("create_draft", record))
        return {
            "id": "rec-1",
            "is_published": False,
            "status": "draft",
            "links": {"self_html": "https://repository.cern/uploads/rec-1"},
        }

    def draft_file_status(self, record_id, key):
        return self.file_status

    def delete_draft_file(self, record_id, key):
        self.writes.append(("delete_draft_file", key))

    def upload_file(self, record_id, key, stream, size):
        if self.fail_upload:
            raise OSError("upload failed")
        self.writes.append(("upload_file", key, stream.read(), size))

    def submit_to_community(self, record_id, community_id):
        self.writes.append(("submit", record_id, community_id))
        return {"id": "req-1"}

    def accept_request(self, request_id):
        if self.accept_error:
            raise self.accept_error
        self.writes.append(("accept", request_id))

    def review_request_id(self, record_id):
        return "req-9"

    def get_record(self, record_id):
        return published_record()

    def record_file_keys(self, record_id):
        return self.file_keys

    def new_version(self, record_id, update):
        new_metadata = update({"title": "Old", "subjects": [{"subject": "S"}]})
        self.writes.append(("new_version", record_id, new_metadata))
        return {"id": "rec-2", "is_published": False, "status": "new_version_draft"}

    def file_checksums(self, record_id, draft=False):
        checksum = self.uploaded_checksum if draft else "md5:old"
        return {"a-slug.pdf": checksum}

    def publish_draft(self, record_id):
        self.writes.append(("publish", record_id))
        return {
            "id": record_id,
            "is_published": True,
            "links": {"self_html": RECORD2_HTML},
        }

    def delete_draft(self, record_id):
        self.writes.append(("delete_draft", record_id))

    def edit_published_record(self, record_id, update):
        new_metadata = update({"title": "Old", "subjects": [{"subject": "S"}]})
        self.writes.append(("edit", record_id, new_metadata))
        return published_record()

    @staticmethod
    def file_url(record, key):
        return f"{record['links']['self_html']}/files/{key}"


class FakeStream:
    def __init__(self):
        self.closed = False

    def read(self):
        return b"%PDF"

    def close(self):
        self.closed = True


class FakeS3:
    def __init__(self, fail_move=False):
        self.streams = []
        self.moves = []
        self.fail_move = fail_move

    def open_stream(self, key):
        stream = FakeStream()
        self.streams.append(stream)
        return stream, 4

    def move_file(self, source, destination):
        if self.fail_move:
            raise OSError("Access Denied")
        self.moves.append((source, destination))


def published_record():
    return {
        "id": "rec-1",
        "is_published": True,
        "status": "published",
        "links": {"self_html": RECORD_HTML},
    }


def settings(
    mode=DEPOSIT_AND_LINK, auto_accept=False, refresh_metadata=False, replace_file=False
):
    return PublishSettings(
        mode=mode,
        community_id="uuid-1",
        auto_accept=auto_accept,
        resource_type="publication-other",
        refresh_metadata=refresh_metadata,
        replace_file=replace_file,
    )


def run(
    atom,
    repo,
    s3=None,
    mode=DEPOSIT_AND_LINK,
    auto_accept=False,
    can_deposit=True,
    refresh_metadata=False,
    replace_file=False,
):
    return publisher.publish_slug(
        "a-slug",
        "raw/a-slug.pdf",
        s3 or FakeS3(),
        atom,
        repo,
        settings(mode, auto_accept, refresh_metadata, replace_file),
        can_deposit=can_deposit,
    )


def test_no_atom_description():
    row = run(FakeAtom({}), FakeRepository())

    assert row["status"] == Status.NO_ATOM_DESCRIPTION


def test_already_linked_is_skipped():
    repo = FakeRepository()
    row = run(
        FakeAtom({"a-slug": {**DESCRIPTION, "digital_object": {"url": "x"}}}), repo
    )

    assert row["status"] == Status.ALREADY_LINKED
    assert repo.writes == []


def test_new_deposit_is_submitted_for_review():
    s3, repo, atom = FakeS3(), FakeRepository(), FakeAtom({"a-slug": DESCRIPTION})

    row = run(atom, repo, s3)

    assert row["status"] == Status.PENDING_REVIEW
    assert row["new_deposit"] is True
    assert row["record_url"] == "https://repository.cern/uploads/rec-1"
    assert [w[0] for w in repo.writes] == ["create_draft", "upload_file", "submit"]
    assert repo.writes[1] == ("upload_file", "a-slug.pdf", b"%PDF", 4)
    assert repo.writes[2] == ("submit", "rec-1", "uuid-1")
    assert s3.streams[0].closed
    assert atom.attached == []


def test_new_deposit_with_auto_accept_is_linked():
    repo, atom = FakeRepository(), FakeAtom({"a-slug": DESCRIPTION})

    row = run(atom, repo, auto_accept=True)

    assert row["status"] == Status.LINKED
    assert ("accept", "req-1") in repo.writes
    assert atom.attached == [("a-slug", FILE_URL)]
    assert row["file_url"] == FILE_URL


def test_deposit_only_does_not_update_atom():
    atom = FakeAtom({"a-slug": DESCRIPTION})

    row = run(atom, FakeRepository(), mode=DEPOSIT_ONLY, auto_accept=True)

    assert row["status"] == Status.PUBLISHED
    assert atom.attached == []


def test_published_record_is_reused_and_linked():
    repo = FakeRepository(existing={f"{ATOM_BASE}/a-slug": published_record()})
    atom = FakeAtom({"a-slug": DESCRIPTION})

    row = run(atom, repo)

    assert row["status"] == Status.LINKED
    assert row["new_deposit"] is False
    assert repo.writes == []
    assert atom.attached == [("a-slug", FILE_URL)]


def test_published_record_with_another_single_file_key():
    repo = FakeRepository(
        existing={f"{ATOM_BASE}/a-slug": published_record()}, file_keys=["renamed.pdf"]
    )
    atom = FakeAtom({"a-slug": DESCRIPTION})

    run(atom, repo)

    assert atom.attached == [("a-slug", f"{RECORD_HTML}/files/renamed.pdf")]


def test_published_record_without_the_file_is_an_error():
    repo = FakeRepository(
        existing={f"{ATOM_BASE}/a-slug": published_record()},
        file_keys=["x.pdf", "y.pdf"],
    )
    atom = FakeAtom({"a-slug": DESCRIPTION})

    row = run(atom, repo)

    assert row["status"] == Status.ERROR
    assert "a-slug.pdf" in row["error"]
    assert atom.attached == []


def in_review_record():
    return {
        "id": "rec-1",
        "is_published": False,
        "status": "in_review",
        "links": {"self_html": "https://repository.cern/uploads/rec-1"},
    }


def test_record_in_review_stays_pending():
    repo = FakeRepository(existing={f"{ATOM_BASE}/a-slug": in_review_record()})

    row = run(FakeAtom({"a-slug": DESCRIPTION}), repo)

    assert row["status"] == Status.PENDING_REVIEW
    assert repo.writes == []


def test_record_in_review_is_accepted_with_auto_accept():
    repo = FakeRepository(existing={f"{ATOM_BASE}/a-slug": in_review_record()})
    atom = FakeAtom({"a-slug": DESCRIPTION})

    row = run(atom, repo, auto_accept=True)

    assert repo.writes == [("accept", "req-9")]
    assert row["status"] == Status.LINKED


@pytest.mark.parametrize("review_status", ["declined", "expired"])
def test_declined_review_needs_manual_action(review_status):
    record = {**in_review_record(), "status": review_status}
    repo = FakeRepository(existing={f"{ATOM_BASE}/a-slug": record})

    row = run(FakeAtom({"a-slug": DESCRIPTION}), repo, auto_accept=True)

    assert row["status"] == Status.REVIEW_DECLINED
    assert repo.writes == []


@pytest.mark.parametrize(
    "file_status, expected_writes",
    [
        (None, ["upload_file", "submit"]),
        ("pending", ["delete_draft_file", "upload_file", "submit"]),
        ("completed", ["submit"]),
    ],
)
def test_unfinished_draft_is_resumed(file_status, expected_writes):
    draft = {
        "id": "rec-1",
        "is_published": False,
        "status": "draft_with_review",
        "links": {"self_html": "https://repository.cern/uploads/rec-1"},
    }
    repo = FakeRepository(
        existing={f"{ATOM_BASE}/a-slug": draft}, file_status=file_status
    )

    row = run(FakeAtom({"a-slug": DESCRIPTION}), repo)

    assert [w[0] for w in repo.writes] == expected_writes
    assert row["status"] == Status.PENDING_REVIEW
    assert row["new_deposit"] is False


def test_missing_date_is_reported():
    repo = FakeRepository()

    row = run(FakeAtom({"a-slug": {"title": "T", "dates": []}}), repo)

    assert row["status"] == Status.NO_DATE
    assert repo.writes == []


def test_deposit_budget_used_up_defers_new_records():
    repo = FakeRepository()

    row = run(FakeAtom({"a-slug": DESCRIPTION}), repo, can_deposit=False)

    assert row["status"] == Status.DEFERRED
    assert repo.writes == []


def test_dry_run_never_writes():
    repo, atom = FakeRepository(), FakeAtom({"a-slug": DESCRIPTION})
    assert (
        run(atom, repo, mode=DRY_RUN, auto_accept=True)["status"]
        == Status.WOULD_DEPOSIT
    )

    repo = FakeRepository(existing={f"{ATOM_BASE}/a-slug": in_review_record()})
    assert (
        run(atom, repo, mode=DRY_RUN, auto_accept=True)["status"]
        == Status.PENDING_REVIEW
    )

    repo = FakeRepository(existing={f"{ATOM_BASE}/a-slug": published_record()})
    row = run(atom, repo, mode=DRY_RUN)
    assert row["status"] == Status.PUBLISHED
    assert row["file_url"] == FILE_URL

    assert repo.writes == []
    assert atom.attached == []


def test_dry_run_reports_missing_date():
    row = run(FakeAtom({"a-slug": {"title": "T"}}), FakeRepository(), mode=DRY_RUN)

    assert row["status"] == Status.NO_DATE


def test_unexpected_errors_are_reported_per_slug():
    class BrokenAtom(FakeAtom):
        def get_description(self, slug):
            raise RuntimeError("HTTP 500")

    row = run(BrokenAtom({}), FakeRepository())

    assert row["status"] == Status.ERROR
    assert row["error"] == "HTTP 500"


def test_publish_all_spends_the_deposit_budget_on_new_records_only():
    descriptions = {s: DESCRIPTION for s in ("a", "b", "c")}
    descriptions["a"] = {**DESCRIPTION, "digital_object": {"url": "x"}}
    items = [{"slug": s, "s3_key": f"raw/{s}.pdf"} for s in ("a", "b", "c")]

    rows = publisher.publish_all(
        items,
        FakeS3(),
        FakeAtom(descriptions),
        FakeRepository(),
        settings(),
        max_deposits=1,
    )

    assert [r["status"] for r in rows] == [
        Status.ALREADY_LINKED,
        Status.PENDING_REVIEW,
        Status.DEFERRED,
    ]


def test_plan_items_maps_file_names_to_slugs():
    keys = [
        "raw/council-minutes-1954.pdf",
        "raw/sub/council-minutes-1954.pdf",
        "raw/dg-office-1985.pdf",
        "raw/Bad Name.pdf",
        "raw/archive.tar.pdf",
    ]

    plan = publisher.plan_items(keys)

    assert plan["items"] == [
        {"slug": "dg-office-1985", "s3_key": "raw/dg-office-1985.pdf"}
    ]
    assert sorted((r["slug"], r["status"]) for r in plan["skipped"]) == [
        ("archive.tar", Status.INVALID_SLUG),
        ("bad name", Status.INVALID_SLUG),
        ("council-minutes-1954", Status.DUPLICATE_SLUG),
        ("council-minutes-1954", Status.DUPLICATE_SLUG),
    ]


def test_plan_items_filters_slugs():
    keys = ["raw/a.pdf", "raw/b.pdf"]

    plan = publisher.plan_items(keys, only_slugs=["b", "missing"])

    assert plan["items"] == [{"slug": "b", "s3_key": "raw/b.pdf"}]
    assert plan["skipped"] == [
        {
            "slug": "missing",
            "s3_key": None,
            "status": Status.NOT_IN_S3,
            "record_url": None,
            "file_url": None,
            "error": None,
            "new_deposit": False,
            "moved_to": None,
            "atom_url": None,
            "record_update": None,
        }
    ]


def test_summarize_counts_statuses():
    rows = [
        {"status": Status.LINKED},
        {"status": Status.LINKED},
        {"status": Status.ERROR},
    ]

    assert publisher.summarize(rows) == {Status.LINKED: 2, Status.ERROR: 1}


def test_atom_refusing_an_existing_digital_object_counts_as_already_linked():
    class LinkedAtom(FakeAtom):
        def attach_digital_object(self, slug, uri):
            raise AtomAlreadyLinkedError("Already has a digital object")

    repo = FakeRepository(existing={f"{ATOM_BASE}/a-slug": published_record()})

    row = run(LinkedAtom({"a-slug": DESCRIPTION}), repo)

    assert row["status"] == Status.ALREADY_LINKED
    assert row["record_url"] == RECORD_HTML


class FailingUploadRepository(FakeRepository):
    def upload_file(self, record_id, key, stream, size):
        raise RuntimeError("HTTP 500 upload")


def test_failed_new_deposit_counts_against_the_budget():
    items = [{"slug": s, "s3_key": f"raw/{s}.pdf"} for s in ("a", "b")]

    rows = publisher.publish_all(
        items,
        FakeS3(),
        FakeAtom({"a": DESCRIPTION, "b": DESCRIPTION}),
        FailingUploadRepository(),
        settings(),
        max_deposits=1,
    )

    assert rows[0]["status"] == Status.ERROR
    assert rows[0]["new_deposit"] is True
    assert rows[0]["error"] == "HTTP 500 upload"
    assert rows[1]["status"] == Status.DEFERRED


def test_run_stops_after_errors_in_a_row():
    limit = publisher.config.MAX_CONSECUTIVE_ERRORS
    slugs = [f"s{i}" for i in range(limit + 2)]
    items = [{"slug": s, "s3_key": f"raw/{s}.pdf"} for s in slugs]

    rows = publisher.publish_all(
        items,
        FakeS3(),
        FakeAtom(dict.fromkeys(slugs, DESCRIPTION)),
        FailingUploadRepository(),
        settings(),
        max_deposits=100,
    )

    assert [row["status"] for row in rows] == [Status.ERROR] * limit + [
        Status.DEFERRED
    ] * 2
    assert "errors in a row" in rows[-1]["error"]


def test_slug_with_a_trailing_newline_is_invalid():
    plan = publisher.plan_items(["raw/a-slug\n.pdf"])

    assert plan["items"] == []
    assert plan["skipped"][0]["status"] == Status.INVALID_SLUG


def rows_with(*statuses):
    return [{"status": status} for status in statuses]


@pytest.mark.parametrize(
    "rows, expected",
    [
        (rows_with(Status.LINKED, Status.NO_ATOM_DESCRIPTION), []),
        (rows_with(), []),
        (rows_with(Status.INVALID_SLUG), []),
        (rows_with(Status.LINKED, Status.ERROR), ["1 files failed"]),
        (
            [{"status": Status.LINKED, "error": "Linked, but not moved"}],
            ["1 linked files were not moved"],
        ),
        (
            rows_with(Status.NO_ATOM_DESCRIPTION, Status.INVALID_SLUG),
            ["No file has an AtoM description"],
        ),
    ],
)
def test_run_problems(rows, expected):
    problems = publisher.run_problems(rows)

    assert len(problems) == len(expected)
    for problem, start in zip(problems, expected, strict=True):
        assert problem.startswith(start)


def test_plan_items_lowercases_file_names():
    """AtoM slugs are lowercase, file names are often not: CERN-ARCH-Test-001.pdf."""
    plan = publisher.plan_items(["atom test/CERN-ARCH-Test-001.pdf"])

    assert plan["items"] == [
        {"slug": "cern-arch-test-001", "s3_key": "atom test/CERN-ARCH-Test-001.pdf"}
    ]


def test_plan_items_reports_two_extensions_of_one_slug():
    keys = ["t/CERN-ARCH-Test-001.pdf", "t/cern-arch-test-001.jpg"]

    plan = publisher.plan_items(keys)

    assert plan["items"] == []
    assert {r["status"] for r in plan["skipped"]} == {Status.DUPLICATE_SLUG}


def published_repository():
    return FakeRepository(existing={f"{ATOM_BASE}/a-slug": published_record()})


def publish_one(s3, done_folder="t/migrated/"):
    return publisher.publish_all(
        [{"slug": "a-slug", "s3_key": "t/sub/a-slug.pdf"}],
        s3,
        FakeAtom({"a-slug": DESCRIPTION}),
        published_repository(),
        PublishSettings(
            mode=DEPOSIT_AND_LINK,
            community_id="uuid-1",
            auto_accept=False,
            source_folder="t/",
            done_folder=done_folder,
        ),
        max_deposits=1,
    )


def test_linked_files_are_moved_to_the_done_folder():
    s3 = FakeS3()

    [row] = publish_one(s3)

    assert s3.moves == [("t/sub/a-slug.pdf", "t/migrated/sub/a-slug.pdf")]
    assert row["status"] == Status.LINKED
    assert row["moved_to"] == "t/migrated/sub/a-slug.pdf"
    assert row["error"] is None


def test_files_stay_in_place_without_a_done_folder():
    s3 = FakeS3()

    [row] = publish_one(s3, done_folder=None)

    assert s3.moves == []
    assert row["moved_to"] is None


def test_files_that_are_not_linked_are_not_moved():
    s3 = FakeS3()
    rows = publisher.publish_all(
        [{"slug": "a-slug", "s3_key": "t/a-slug.pdf"}],
        s3,
        FakeAtom({"a-slug": DESCRIPTION}),
        FakeRepository(),
        PublishSettings(
            mode=DEPOSIT_AND_LINK,
            community_id="uuid-1",
            auto_accept=False,
            source_folder="t/",
            done_folder="t/migrated/",
        ),
        max_deposits=1,
    )

    assert rows[0]["status"] == Status.PENDING_REVIEW
    assert s3.moves == []


def test_a_failed_move_keeps_the_link_and_reports_the_error():
    [row] = publish_one(FakeS3(fail_move=True))

    assert row["status"] == Status.LINKED
    assert row["moved_to"] is None
    assert "not moved to t/migrated/sub/a-slug.pdf" in row["error"]
    assert "Access Denied" in row["error"]


def test_rows_carry_the_atom_url():
    row = run(FakeAtom({"a-slug": DESCRIPTION}), FakeRepository())

    assert row["atom_url"] == f"{ATOM_BASE}/a-slug"


def test_rows_without_an_atom_description_have_no_atom_url():
    assert run(FakeAtom({}), FakeRepository())["atom_url"] is None


def test_link_lines_list_the_links_of_each_file():
    rows = [
        {
            **publisher.result("a", "t/a.pdf", Status.LINKED),
            "record_url": RECORD_HTML,
            "file_url": FILE_URL,
            "atom_url": f"{ATOM_BASE}/a",
            "moved_to": "t/migrated/a.pdf",
        },
        {
            **publisher.result("b", "t/b.pdf", Status.PENDING_REVIEW),
            "record_url": "https://repository.cern/uploads/rec-2",
            "atom_url": f"{ATOM_BASE}/b",
        },
        publisher.result("c", "t/c.pdf", Status.NO_ATOM_DESCRIPTION),
    ]

    assert publisher.link_lines(rows) == [
        f"a [linked]\n  CDS record: {RECORD_HTML}\n  CDS file:   {FILE_URL}\n"
        f"  AtoM:       {ATOM_BASE}/a\n  S3:         t/migrated/a.pdf",
        "b [pending_review]\n  CDS record: https://repository.cern/uploads/rec-2\n"
        f"  AtoM:       {ATOM_BASE}/b\n  S3:         t/b.pdf",
        "c [no_atom_description]\n  S3:         t/c.pdf",
    ]


LINKED_DESCRIPTION = {**DESCRIPTION, "digital_object": {"url": FILE_URL}}


def test_refresh_updates_the_published_record_and_links_it():
    repo, atom = published_repository(), FakeAtom({"a-slug": DESCRIPTION})

    row = run(atom, repo, refresh_metadata=True)

    [(action, record_id, new_metadata)] = repo.writes
    assert (action, record_id) == ("edit", "rec-1")
    assert new_metadata["title"] == "T"
    assert new_metadata["subjects"] == [{"subject": "S"}]
    assert row["record_update"] == "refreshed"
    assert row["status"] == Status.LINKED
    assert atom.attached == [("a-slug", FILE_URL)]


def test_refresh_works_when_atom_is_already_linked():
    repo, atom = published_repository(), FakeAtom({"a-slug": LINKED_DESCRIPTION})

    row = run(atom, repo, refresh_metadata=True)

    assert [w[0] for w in repo.writes] == ["edit"]
    assert row["status"] == Status.ALREADY_LINKED
    assert row["record_update"] == "refreshed"
    assert row["record_url"] == RECORD_HTML
    assert atom.attached == []


def test_without_refresh_an_already_linked_record_is_not_read():
    repo = published_repository()

    row = run(FakeAtom({"a-slug": LINKED_DESCRIPTION}), repo)

    assert row["record_update"] is None
    assert repo.writes == []


def test_refresh_in_dry_run_only_reports():
    repo = published_repository()

    row = run(
        FakeAtom({"a-slug": LINKED_DESCRIPTION}),
        repo,
        mode=DRY_RUN,
        refresh_metadata=True,
    )

    assert row["record_update"] == "would_refresh"
    assert repo.writes == []


def test_refresh_skips_records_that_are_not_published():
    repo = FakeRepository(existing={f"{ATOM_BASE}/a-slug": in_review_record()})

    row = run(FakeAtom({"a-slug": LINKED_DESCRIPTION}), repo, refresh_metadata=True)

    assert row["status"] == Status.ALREADY_LINKED
    assert row["record_update"] is None
    assert repo.writes == []


def test_new_records_are_not_refreshed():
    repo = FakeRepository()

    row = run(FakeAtom({"a-slug": DESCRIPTION}), repo, refresh_metadata=True)

    assert row["record_update"] is None
    assert "edit" not in [w[0] for w in repo.writes]


def test_files_already_in_the_done_folder_stay_there():
    s3 = FakeS3()
    rows = publisher.publish_all(
        [{"slug": "a-slug", "s3_key": "t/migrated/a-slug.pdf"}],
        s3,
        FakeAtom({"a-slug": DESCRIPTION}),
        published_repository(),
        PublishSettings(
            mode=DEPOSIT_AND_LINK,
            community_id="uuid-1",
            auto_accept=False,
            source_folder="t/",
            done_folder="t/migrated/",
        ),
        max_deposits=1,
    )

    assert rows[0]["status"] == Status.LINKED
    assert s3.moves == []
    assert rows[0]["moved_to"] is None


def test_summarize_counts_the_record_updates():
    rows = [
        {"status": Status.LINKED, "record_update": "refreshed"},
        {"status": Status.ALREADY_LINKED, "record_update": "refreshed"},
        {"status": Status.ALREADY_LINKED, "record_update": None},
    ]

    assert publisher.summarize(rows) == {
        Status.LINKED: 1,
        Status.ALREADY_LINKED: 2,
        "record_refreshed": 2,
    }


def test_replace_file_publishes_a_new_version_and_links_it():
    repo, atom = published_repository(), FakeAtom({"a-slug": DESCRIPTION})

    row = run(atom, repo, replace_file=True)

    assert [w[0] for w in repo.writes] == ["new_version", "upload_file", "publish"]
    assert repo.writes[0][2]["title"] == "T"
    assert repo.writes[0][2]["subjects"] == [{"subject": "S"}]
    assert row["record_update"] == "new_version"
    assert row["record_url"] == RECORD2_HTML
    assert row["status"] == Status.LINKED
    assert atom.attached == [("a-slug", f"{RECORD2_HTML}/files/a-slug.pdf")]


def test_replace_file_when_atom_is_already_linked_warns(caplog):
    repo = published_repository()
    atom = FakeAtom({"a-slug": LINKED_DESCRIPTION})

    row = run(atom, repo, replace_file=True)

    assert [w[0] for w in repo.writes] == ["new_version", "upload_file", "publish"]
    assert row["status"] == Status.ALREADY_LINKED
    assert row["record_update"] == "new_version"
    assert atom.attached == []
    assert "previous version" in caplog.text


def test_replace_file_with_the_same_file_only_refreshes():
    """No new version for an identical file: every version stays public."""
    repo = FakeRepository(
        existing={f"{ATOM_BASE}/a-slug": published_record()},
        uploaded_checksum="md5:old",
    )

    row = run(FakeAtom({"a-slug": DESCRIPTION}), repo, replace_file=True)

    assert [w[0] for w in repo.writes] == [
        "new_version",
        "upload_file",
        "delete_draft",
        "edit",
    ]
    assert row["record_update"] == "refreshed"
    assert row["record_url"] == RECORD_HTML


def test_replace_file_discards_the_version_when_the_upload_fails():
    repo = FakeRepository(
        existing={f"{ATOM_BASE}/a-slug": published_record()}, fail_upload=True
    )

    row = run(FakeAtom({"a-slug": DESCRIPTION}), repo, replace_file=True)

    assert row["status"] == Status.ERROR
    assert [w[0] for w in repo.writes] == ["new_version", "delete_draft"]


def test_replace_file_in_dry_run_only_reports():
    repo = published_repository()

    row = run(
        FakeAtom({"a-slug": LINKED_DESCRIPTION}), repo, mode=DRY_RUN, replace_file=True
    )

    assert row["record_update"] == "would_replace_file"
    assert repo.writes == []


def test_replace_file_does_not_apply_to_new_records():
    repo = FakeRepository()

    row = run(FakeAtom({"a-slug": DESCRIPTION}), repo, replace_file=True)

    assert row["record_update"] is None
    assert "new_version" not in [w[0] for w in repo.writes]


FORBIDDEN = HttpError("HTTP 403 .../actions/accept: Permission denied.", 403)


def test_a_refused_accept_leaves_the_new_record_in_review(caplog):
    """The token is not a curator: the record is submitted, a curator accepts it."""
    repo = FakeRepository(accept_error=FORBIDDEN)

    row = run(FakeAtom({"a-slug": DESCRIPTION}), repo, auto_accept=True)

    assert row["status"] == Status.PENDING_REVIEW
    assert row["record_url"] == "https://repository.cern/uploads/rec-1"
    assert row["new_deposit"] is True
    assert row["error"] is None
    assert "not a curator" in caplog.text


def test_a_refused_accept_of_a_record_in_review(caplog):
    repo = FakeRepository(
        existing={f"{ATOM_BASE}/a-slug": in_review_record()}, accept_error=FORBIDDEN
    )

    row = run(FakeAtom({"a-slug": DESCRIPTION}), repo, auto_accept=True)

    assert row["status"] == Status.PENDING_REVIEW
    assert row["record_url"] == "https://repository.cern/uploads/rec-1"
    assert "not a curator" in caplog.text


def test_other_accept_errors_still_fail():
    repo = FakeRepository(accept_error=HttpError("HTTP 500 ...", 500))

    row = run(FakeAtom({"a-slug": DESCRIPTION}), repo, auto_accept=True)

    assert row["status"] == Status.ERROR


ARCHIVE_SLUG = "cern-arch-test-002"
ARCHIVE_URL = f"{ATOM_BASE}/{ARCHIVE_SLUG}"


def test_a_record_created_with_the_atom_url_is_found_and_moved_to_the_archive_scheme():
    """Older records have the AtoM URL; a refresh replaces it with CERN Archives."""
    repo = FakeRepository(existing={ARCHIVE_URL: published_record()})
    atom = FakeAtom({ARCHIVE_SLUG: {**DESCRIPTION, "digital_object": {"url": "x"}}})

    row = publisher.publish_slug(
        ARCHIVE_SLUG,
        f"raw/{ARCHIVE_SLUG}.pdf",
        FakeS3(),
        atom,
        repo,
        settings(refresh_metadata=True),
    )

    assert repo.searched == ("CERN-ARCH-TEST-002", ARCHIVE_URL)
    assert row["record_update"] == "refreshed"
    [(_, _, new_metadata)] = repo.writes
    assert [
        (r["identifier"], r["scheme"]) for r in new_metadata["related_identifiers"]
    ] == [("CERN-ARCH-TEST-002", "archive")]
