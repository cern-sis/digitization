import json

import pytest

from digitization_workflows.repository_publish import (
    publish_to_repository,
    run_settings,
)
from digitization_workflows.repository_publish.publish_to_repository import (
    publish_to_repository_dag_instance as dag,
)


def test_dag_loads_with_expected_tasks():
    assert dag.dag_id == "publish_to_repository"
    assert "digitization" in dag.tags
    assert set(dag.task_ids) == {"list_s3_files", "publish", "report"}


def test_dag_defaults_are_safe():
    assert dag.params["execution_mode"] == "Dry Run"
    assert dag.params.get_param("execution_mode").schema["enum"] == [
        "Dry Run",
        "Deposit only",
        "Deposit and link",
    ]
    assert dag.params["auto_accept"] is False


def test_form_shows_the_defaults_without_a_variable():
    """No Variable when the tests parse the DAG: the code defaults only."""
    assert dag.params["extension"] == ".pdf"
    assert dag.params["resource_type"] == "publication"
    assert dag.params["done_folder"] == "migrated"
    assert dag.params["max_deposits"] == 50
    for key in run_settings.REQUIRED:
        assert dag.params[key] is None, key


PARAMS = {
    "prefix": "/cern-archives/repository",
    "bucket": "digitization-dev",
    "extension": ".pdf, JPG",
    "slugs": " a , B ",
    "max_deposits": 5,
    "community": "cern-digitized-archives-special-collections",
    "auto_accept": False,
    "resource_type": "publication-other",
    "execution_mode": "Dry Run",
    "upload_report": True,
    "done_folder": "migrated",
    "reprocess_migrated": False,
    "refresh_metadata": False,
    "replace_file": False,
}


class FakeS3Provider:
    instances = []
    extra_keys = []

    def __init__(self, bucket, conn_id):
        self.bucket = bucket
        self.listed = []
        self.uploaded = []
        FakeS3Provider.instances.append(self)

    def list_files(self, folder, extension=None):
        self.listed.append((folder, extension))
        return [
            f"{folder}a.pdf",
            f"{folder}B.jpg",
            f"{folder}c.pdf",
            f"{folder}d.tif",
            f"{folder}migrated/e.pdf",
            f"{folder}migrated/a.pdf",
        ] + [f"{folder}{key}" for key in FakeS3Provider.extra_keys]

    def upload_file(self, local_path, remote_path):
        with open(local_path) as report:
            self.uploaded.append((remote_path, json.load(report)))


class FakeRepositoryClient:
    def community_id(self, slug):
        return f"id-of-{slug}"


ENVIRONMENT = {}
# The real function, before the fixture replaces it.
READ_ENVIRONMENT_SETTINGS = publish_to_repository._environment_settings


@pytest.fixture(autouse=True)
def fake_providers(monkeypatch):
    FakeS3Provider.instances = []
    ENVIRONMENT.clear()
    monkeypatch.setattr(
        publish_to_repository, "_environment_settings", lambda: dict(ENVIRONMENT)
    )
    monkeypatch.setattr(publish_to_repository, "S3Provider", FakeS3Provider)
    monkeypatch.setattr(publish_to_repository, "RepositoryClient", FakeRepositoryClient)
    monkeypatch.setattr(publish_to_repository, "AtomClient", lambda: "atom")


def test_list_s3_files_keeps_the_selected_slugs():
    plan = dag.get_task("list_s3_files").python_callable(params=PARAMS)

    assert FakeS3Provider.instances[0].listed == [("cern-archives/repository/", None)]
    assert [item["slug"] for item in plan["items"]] == ["a", "b"]
    assert plan["items"][1]["s3_key"] == "cern-archives/repository/B.jpg"
    assert plan["skipped"] == []
    assert plan["settings"] == run_settings.resolve(PARAMS, {})


def test_list_s3_files_uses_the_variable_for_empty_params():
    ENVIRONMENT.update(bucket="env-bucket", prefix="env/folder", community="c")
    params = {**PARAMS, "bucket": None, "prefix": None, "community": None}

    plan = dag.get_task("list_s3_files").python_callable(params=params)

    assert FakeS3Provider.instances[0].bucket == "env-bucket"
    assert FakeS3Provider.instances[0].listed == [("env/folder/", None)]
    assert plan["settings"]["community"] == "c"


def test_list_s3_files_fails_without_settings():
    params = {**PARAMS, "bucket": None}

    with pytest.raises(run_settings.SettingsError, match="'bucket'"):
        dag.get_task("list_s3_files").python_callable(params=params)


def plan_with(items=(), skipped=(), **overrides):
    settings = run_settings.resolve({**PARAMS, **overrides}, {})
    return {"items": list(items), "skipped": list(skipped), "settings": settings}


def test_publish_passes_the_settings(monkeypatch):
    calls = []

    def fake_publish_all(items, s3, atom, repo, settings, max_deposits):
        calls.append((items, atom, settings, max_deposits))
        return [{"slug": "a", "status": "would_deposit"}]

    monkeypatch.setattr(
        publish_to_repository.publisher, "publish_all", fake_publish_all
    )
    skipped = [{"slug": "x", "status": "invalid_slug"}]

    rows = dag.get_task("publish").python_callable(
        plan_with([{"slug": "a", "s3_key": "k"}], skipped), params=PARAMS
    )

    assert rows == skipped + [{"slug": "a", "status": "would_deposit"}]
    items, atom, settings, max_deposits = calls[0]
    assert atom == "atom"
    assert settings.community_id == "id-of-cern-digitized-archives-special-collections"
    assert settings.is_dry_run
    assert max_deposits == 5
    assert settings.source_folder == "cern-archives/repository/"
    assert settings.done_folder == "cern-archives/repository/migrated/"


def row(status, error=None):
    return {"slug": "a", "s3_key": "k", "status": status, "error": error}


def test_report_uploads_the_summary():
    summary = dag.get_task("report").python_callable(
        [row("linked"), row("no_date", "no year")],
        plan_with(),
        params=PARAMS,
        run_id="manual__2026-11-26T10:00:00+00:00",
    )

    assert summary == {"linked": 1, "no_date": 1}
    remote_path, content = FakeS3Provider.instances[0].uploaded[0]
    assert remote_path == (
        "cern-archives/repository/reports/"
        "publish_report_manual__2026-11-26T10_00_00_00_00.json"
    )
    assert content["summary"] == summary


def test_report_fails_the_run_when_files_failed():
    with pytest.raises(RuntimeError, match="1 files failed"):
        dag.get_task("report").python_callable(
            [row("error", "HTTP 500")],
            plan_with(),
            params={**PARAMS, "upload_report": False},
            run_id="r",
        )


@pytest.mark.parametrize("prefix", ["/", "  "])
def test_list_s3_files_refuses_an_empty_prefix(prefix):
    with pytest.raises(ValueError, match="prefix"):
        dag.get_task("list_s3_files").python_callable(
            params={**PARAMS, "prefix": prefix}
        )


def test_report_fails_when_no_file_has_an_atom_description():
    with pytest.raises(RuntimeError, match="No file has an AtoM description"):
        dag.get_task("report").python_callable(
            [row("no_atom_description")],
            plan_with(),
            params={**PARAMS, "upload_report": False},
            run_id="r",
        )


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (".pdf", (".pdf",)),
        ("pdf, .JPG ,", (".pdf", ".jpg")),
    ],
)
def test_extension_list(value, expected):
    assert publish_to_repository._extensions(value) == expected


@pytest.mark.parametrize("value", ["", " , "])
def test_extension_list_must_not_be_empty(value):
    with pytest.raises(ValueError, match="extension"):
        publish_to_repository._extensions(value)


def test_list_s3_files_skips_the_done_folder():
    plan = dag.get_task("list_s3_files").python_callable(
        params={**PARAMS, "slugs": None}
    )

    assert [item["slug"] for item in plan["items"]] == ["a", "b", "c"]


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("migrated", "t/migrated/"),
        ("/done/2026/", "t/done/2026/"),
        (None, None),
        ("  ", None),
    ],
)
def test_done_folder(value, expected):
    assert publish_to_repository._done_folder("t/", value) == expected


@pytest.mark.parametrize("value", ["../elsewhere", "a/../../b"])
def test_done_folder_must_stay_inside_the_prefix(value):
    with pytest.raises(ValueError, match="done_folder"):
        publish_to_repository._done_folder("t/", value)


class FakeVariable:
    value = None

    @classmethod
    def get(cls, key, default=None, deserialize_json=False):
        assert key == "publish_to_repository"
        assert deserialize_json
        if isinstance(cls.value, Exception):
            raise cls.value
        return default if cls.value is None else cls.value


@pytest.mark.parametrize(
    ("value", "expected"), [(None, None), ({"bucket": "b"}, {"bucket": "b"})]
)
def test_environment_settings_read_the_variable(monkeypatch, value, expected):
    monkeypatch.setattr(FakeVariable, "value", value)
    monkeypatch.setattr(publish_to_repository, "Variable", FakeVariable)

    assert READ_ENVIRONMENT_SETTINGS() == expected


def test_environment_settings_refuse_invalid_json(monkeypatch):
    monkeypatch.setattr(FakeVariable, "value", ValueError("Expecting value"))
    monkeypatch.setattr(publish_to_repository, "Variable", FakeVariable)

    with pytest.raises(run_settings.SettingsError, match="not valid JSON"):
        READ_ENVIRONMENT_SETTINGS()


def test_reprocess_options_are_off_by_default():
    assert dag.params["reprocess_migrated"] is False
    assert dag.params["refresh_metadata"] is False


def test_reprocess_migrated_includes_the_done_folder_for_the_slugs():
    params = {**PARAMS, "slugs": "e", "reprocess_migrated": True}

    plan = dag.get_task("list_s3_files").python_callable(params=params)

    assert plan["items"] == [
        {"slug": "e", "s3_key": "cern-archives/repository/migrated/e.pdf"}
    ]


def test_reprocess_migrated_needs_slugs():
    params = {**PARAMS, "slugs": None, "reprocess_migrated": True}

    with pytest.raises(ValueError, match="slugs"):
        dag.get_task("list_s3_files").python_callable(params=params)


def test_publish_passes_refresh_metadata(monkeypatch):
    calls = []
    monkeypatch.setattr(
        publish_to_repository.publisher,
        "publish_all",
        lambda items, s3, atom, repo, settings, max_deposits: (
            calls.append(settings) or []
        ),
    )

    dag.get_task("publish").python_callable(
        plan_with(), params={**PARAMS, "refresh_metadata": True}
    )

    assert calls[0].refresh_metadata is True


def test_form_defaults_come_from_the_variable():
    ENVIRONMENT.update(bucket="b", prefix="p")

    defaults = publish_to_repository._form_defaults()

    assert (defaults["bucket"], defaults["prefix"]) == ("b", "p")
    assert defaults["extension"] == ".pdf"


def test_a_broken_variable_does_not_break_the_dag(caplog):
    """The DAG still loads; the run itself reports the error."""
    ENVIRONMENT.update(comunity="typo")

    defaults = publish_to_repository._form_defaults()

    assert defaults == run_settings.defaults(None)
    assert "comunity" in caplog.text


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (" a , B ", ["a", "b"]),
        ("cern-arch-test-002.jpg", ["cern-arch-test-002"]),
        (
            "CERN-ARCH-Test-002.JPG, atom test/migrated/X-1.pdf",
            ["cern-arch-test-002", "x-1"],
        ),
        (None, None),
        (" , ", None),
    ],
)
def test_slugs_accept_file_names(value, expected):
    """A file name in 'slugs' is a common mistake: it selects the file's slug."""
    assert publish_to_repository._slug_list(value) == expected


def test_replace_file_is_off_by_default():
    assert dag.params["replace_file"] is False


def test_replace_file_needs_slugs():
    params = {**PARAMS, "slugs": None, "replace_file": True}

    with pytest.raises(ValueError, match="slugs"):
        dag.get_task("list_s3_files").python_callable(params=params)


def test_reprocess_prefers_a_new_file_to_the_migrated_one():
    """A new file in the folder replaces the migrated one, without duplicate_slug."""
    params = {**PARAMS, "slugs": "a", "reprocess_migrated": True}

    plan = dag.get_task("list_s3_files").python_callable(params=params)

    assert plan["items"] == [{"slug": "a", "s3_key": "cern-archives/repository/a.pdf"}]
    assert plan["skipped"] == []


def test_publish_passes_replace_file(monkeypatch):
    calls = []
    monkeypatch.setattr(
        publish_to_repository.publisher,
        "publish_all",
        lambda items, s3, atom, repo, settings, max_deposits: (
            calls.append(settings) or []
        ),
    )

    dag.get_task("publish").python_callable(
        plan_with(), params={**PARAMS, "replace_file": True}
    )

    assert calls[0].replace_file is True


@pytest.mark.parametrize(
    ("done_folder", "keep_unique", "expected"),
    [
        (None, False, ["t/a.pdf", "t/m/a.pdf", "t/m/b.pdf"]),
        ("t/m/", False, ["t/a.pdf"]),
        ("t/m/", True, ["t/a.pdf", "t/m/b.pdf"]),
    ],
)
def test_without_done_files(done_folder, keep_unique, expected):
    keys = ["t/a.pdf", "t/m/a.pdf", "t/m/b.pdf"]

    assert (
        publish_to_repository._without_done_files(keys, done_folder, keep_unique)
        == expected
    )


def test_an_empty_reports_folder_writes_the_report_in_the_prefix():
    dag.get_task("report").python_callable(
        [row("linked")],
        plan_with(reports_folder=""),
        params=PARAMS,
        run_id="r",
    )

    remote_path, _ = FakeS3Provider.instances[0].uploaded[0]
    assert remote_path == "cern-archives/repository/publish_report_r.json"


def test_the_reports_folder_must_stay_inside_the_prefix():
    with pytest.raises(ValueError, match="reports_folder"):
        dag.get_task("report").python_callable(
            [row("linked")],
            plan_with(reports_folder="../elsewhere"),
            params=PARAMS,
            run_id="r",
        )


def test_reports_are_not_listed_as_files():
    """Even with .json in the extensions, the reports are not published."""
    FakeS3Provider.extra_keys = ["reports/publish_report_r.json", "x.json"]
    try:
        plan = dag.get_task("list_s3_files").python_callable(
            params={**PARAMS, "slugs": None, "extension": ".json"}
        )
    finally:
        FakeS3Provider.extra_keys = []

    assert [item["s3_key"] for item in plan["items"]] == [
        "cern-archives/repository/x.json"
    ]
    assert plan["skipped"] == []
