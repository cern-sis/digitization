import json
import logging
import re
import tempfile
from datetime import datetime

from airflow.sdk import Param, Variable, dag, task

from digitization_workflows.common.storage_connection import S3Provider
from digitization_workflows.repository_publish import config, publisher, run_settings
from digitization_workflows.repository_publish.atom_client import AtomClient
from digitization_workflows.repository_publish.publisher import (
    EXECUTION_MODES,
    PublishSettings,
)
from digitization_workflows.repository_publish.repository_client import (
    RepositoryClient,
)

logger = logging.getLogger(__name__)


def _folder(prefix: str) -> str:
    folder = prefix.strip().strip("/")
    if not folder:
        raise ValueError("Set the S3 folder in the 'prefix' param.")
    return folder + "/"


def _slug_list(value: str | None) -> list[str] | None:
    """'a, B.jpg, folder/C.pdf' -> ['a', 'b', 'c']: file names select their slug."""
    names = [name.strip() for name in (value or "").split(",") if name.strip()]
    return [publisher.slug_from_key(name) for name in names] or None


def _subfolder(folder: str, value: str | None, setting: str) -> str | None:
    """'migrated' -> '<folder>migrated/'. None or empty: no subfolder."""
    name = (value or "").strip().strip("/")
    if not name:
        return None
    if any(part in ("", ".", "..") for part in name.split("/")):
        raise ValueError(
            f"The '{setting}' setting must be a folder inside the prefix: {value!r}"
        )
    return f"{folder}{name}/"


def _done_folder(folder: str, value: str | None) -> str | None:
    """Where the linked files are moved. None: they are not moved."""
    return _subfolder(folder, value, "done_folder")


def _reports_folder(folder: str, value: str | None) -> str:
    """Where the reports are uploaded: the subfolder, or the prefix itself."""
    return _subfolder(folder, value, "reports_folder") or folder


def _without_done_files(
    keys: list[str], done_folder: str | None, keep_unique: bool
) -> list[str]:
    """The keys outside the done folder. With `keep_unique`, also the done files
    whose slug has no new file outside it (a new file replaces the migrated one)."""
    if not done_folder:
        return keys
    is_done = {key: key.startswith(done_folder) for key in keys}
    new_slugs = {publisher.slug_from_key(key) for key in keys if not is_done[key]}
    return [
        key
        for key in keys
        if not is_done[key]
        or (keep_unique and publisher.slug_from_key(key) not in new_slugs)
    ]


def _environment_settings() -> object:
    """The content of the settings Variable of this environment (None if not set)."""
    try:
        return Variable.get(config.SETTINGS_VARIABLE, None, deserialize_json=True)
    except ValueError as error:
        raise run_settings.SettingsError(
            f"The '{config.SETTINGS_VARIABLE}' Variable is not valid JSON: {error}"
        ) from error


def _form_defaults() -> dict:
    """The defaults of the trigger form: the settings Variable when Airflow parses the
    DAG file, else the code defaults. A broken Variable must not break the DAG: the
    run reports it."""
    try:
        return run_settings.defaults(_environment_settings())
    except Exception as error:  # noqa: BLE001 - logged, and the run fails on it
        logger.warning("The trigger form shows the code defaults: %s", error)
        return run_settings.defaults(None)


FORM_DEFAULTS = _form_defaults()


def _environment_param(key: str, param_type: str, description: str, **schema) -> Param:
    """A param whose default is the value of the settings Variable."""
    return Param(
        FORM_DEFAULTS[key],
        type=["null", param_type],
        description=f"{description}. Default: the '{config.SETTINGS_VARIABLE}' "
        "Variable. Empty: the Variable at run time.",
        **schema,
    )


def _extensions(value: str) -> tuple[str, ...]:
    """'.pdf, jpg' -> ('.pdf', '.jpg')."""
    names = [name.strip().lower().lstrip(".") for name in value.split(",")]
    extensions = tuple(f".{name}" for name in names if name)
    if not extensions:
        raise ValueError("Set at least one file extension in the 'extension' param.")
    return extensions


@dag(
    dag_id="publish_to_repository",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["digitization", "repository", "atom"],
    params={
        "bucket": _environment_param("bucket", "string", "S3 bucket"),
        "prefix": _environment_param(
            "prefix",
            "string",
            "S3 folder with the files, named <AtoM slug>.<extension>",
        ),
        "extension": _environment_param(
            "extension",
            "string",
            "Comma-separated extensions of the files, e.g. .pdf,.jpg",
        ),
        "slugs": Param(
            None,
            type=["null", "string"],
            description="Optional comma-separated slugs (or file names): only these "
            "files are processed",
        ),
        "max_deposits": _environment_param(
            "max_deposits",
            "integer",
            "Maximum number of new repository.cern records per run",
            minimum=1,
        ),
        "community": _environment_param(
            "community",
            "string",
            "repository.cern community the records are submitted to",
        ),
        "auto_accept": Param(
            False,
            type="boolean",
            description="Accept the community submissions (the token must be a curator)",
        ),
        "resource_type": _environment_param(
            "resource_type",
            "string",
            "repository.cern resource type of the new records",
        ),
        "execution_mode": Param(
            publisher.DRY_RUN,
            type="string",
            enum=EXECUTION_MODES,
            description="Dry Run only reads. Deposit only creates the records. "
            "Deposit and link also adds the links to AtoM.",
        ),
        "done_folder": _environment_param(
            "done_folder",
            "string",
            "Subfolder of the prefix where the linked files are moved "
            "(an empty string in the Variable: the files stay in place)",
        ),
        "reprocess_migrated": Param(
            False,
            type="boolean",
            description="Also process the files of 'slugs' that are in the done "
            "folder, e.g. to link them again after their digital object was deleted "
            "in AtoM. Needs 'slugs'.",
        ),
        "refresh_metadata": Param(
            False,
            type="boolean",
            description="Update the published repository.cern records with the "
            "current AtoM metadata, even if AtoM is already linked",
        ),
        "replace_file": Param(
            False,
            type="boolean",
            description="Publish a new version of the published repository.cern "
            "records with the S3 file and the current AtoM metadata (nothing if the "
            "file is the same). AtoM is linked to it only if it has no digital "
            "object. Needs 'slugs'.",
        ),
        "reports_folder": _environment_param(
            "reports_folder",
            "string",
            "Subfolder of the prefix for the reports "
            "(an empty string in the Variable: the prefix itself)",
        ),
        "upload_report": Param(
            False,
            type="boolean",
            description="Upload publish_report_<run_id>.json to the reports folder",
        ),
    },
)
def publish_to_repository_dag():
    @task(task_id="list_s3_files")
    def list_s3_files(**context) -> dict:
        params = context["params"]
        settings = run_settings.resolve(params, _environment_settings())
        logger.info("Settings: %s", settings)
        provider = S3Provider(bucket=settings["bucket"], conn_id=config.S3_CONN_ID)
        folder = _folder(settings["prefix"])
        done_folder = _done_folder(folder, settings["done_folder"])
        reports_folder = _reports_folder(folder, settings["reports_folder"])
        extensions = _extensions(settings["extension"])
        slugs = _slug_list(params["slugs"])
        reprocess = params.get("reprocess_migrated", False)
        for option in ("reprocess_migrated", "replace_file"):
            if params.get(option) and not slugs:
                raise ValueError(f"'{option}' needs the 'slugs' of the files.")
        keys = [
            key
            for key in provider.list_files(folder)
            if key.lower().endswith(extensions)
            and not (reports_folder != folder and key.startswith(reports_folder))
        ]
        keys = _without_done_files(keys, done_folder, keep_unique=reprocess)

        plan = publisher.plan_items(keys, slugs)
        logger.info(
            "%d files in S3: %d to process, %d skipped.",
            len(keys),
            len(plan["items"]),
            len(plan["skipped"]),
        )
        # The next tasks use the same settings, even if the Variable changes.
        return {**plan, "settings": settings}

    @task(task_id="publish")
    def publish(plan: dict, **context) -> list[dict]:
        params, run = context["params"], plan["settings"]
        folder = _folder(run["prefix"])
        repo = RepositoryClient()
        settings = PublishSettings(
            mode=params["execution_mode"],
            community_id=repo.community_id(run["community"]),
            auto_accept=params["auto_accept"],
            resource_type=run["resource_type"],
            source_folder=folder,
            done_folder=_done_folder(folder, run["done_folder"]),
            refresh_metadata=params.get("refresh_metadata", False),
            replace_file=params.get("replace_file", False),
        )
        rows = publisher.publish_all(
            plan["items"],
            S3Provider(bucket=run["bucket"], conn_id=config.S3_CONN_ID),
            AtomClient(),
            repo,
            settings,
            max_deposits=run["max_deposits"],
        )
        return plan["skipped"] + rows

    @task(task_id="report")
    def report(rows: list[dict], plan: dict, **context) -> dict:
        params = context["params"]
        summary = publisher.summarize(rows)
        logger.info("Summary (%s): %s", params["execution_mode"], summary)
        for row in rows:
            if row["error"]:
                logger.warning("%s (%s): %s", row["slug"], row["status"], row["error"])

        if params["upload_report"]:
            _upload_report(
                plan["settings"],
                params["execution_mode"],
                context["run_id"],
                summary,
                rows,
            )

        logger.info("Links:\n%s", "\n".join(publisher.link_lines(rows)))

        problems = publisher.run_problems(rows)
        if problems:
            raise RuntimeError(" ".join(problems))
        return summary

    plan = list_s3_files()
    report(publish(plan), plan)


def _upload_report(
    settings: dict, mode: str, run_id: str, summary: dict, rows: list[dict]
) -> None:
    safe_run_id = re.sub(r"[^A-Za-z0-9_.-]", "_", run_id)
    reports_folder = _reports_folder(
        _folder(settings["prefix"]), settings["reports_folder"]
    )
    remote_path = f"{reports_folder}publish_report_{safe_run_id}.json"
    content = {"mode": mode, "summary": summary, "rows": rows}

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json") as tmp:
        json.dump(content, tmp, indent=2)
        tmp.flush()
        S3Provider(bucket=settings["bucket"], conn_id=config.S3_CONN_ID).upload_file(
            tmp.name, remote_path
        )
    logger.info("Report uploaded to s3://%s/%s", settings["bucket"], remote_path)


publish_to_repository_dag_instance = publish_to_repository_dag()
