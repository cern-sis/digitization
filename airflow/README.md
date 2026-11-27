# Digitization – Airflow

Airflow DAGs for the digitization workflows:

| DAG                        | What it does                                                                                                                                             |
| -------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `validate_files_integrity` | Checks that the PDFs of the selected boxes in S3 can be opened (not corrupted).                                                                          |
| `match_and_export`         | Matches the records in the box Excel files (`BOITE_O0<n>.xlsx`) with the files in S3, then builds the MARCXML files.                                     |
| `publish_to_repository`    | Deposits the files of an S3 folder on repository.cern, then links each one to its description in the CERN Archive AtoM (the file name is the AtoM slug). |

Airflow DAGs for the library notifications:

| DAG                  | What it does                                                                                                  |
| -------------------- | ------------------------------------------------------------------------------------------------------------- |
| `library_newsletter` | Every Monday, emails the books/e-books added to the library catalogue in the previous week, grouped by topic. |

### Project layout

Each project has its own folder in `dags/`:

```
dags/
├── digitization_workflows/   # digitization project
│   ├── common/               #   storage providers (S3, CERNBox) and helpers
│   ├── check_files/          #   validate_files_integrity
│   ├── file_import/          #   match_and_export
│   └── repository_publish/   #   publish_to_repository
└── notifications/            # library notifications project (library_newsletter)
```

The `dags/` folder is on the Python path, so imports start with the project folder, for example `from digitization_workflows.common.utils import parse_inventory`. The digitization folder is not called `digitization`, because that name would conflict with the `digitization` package at the root of the repository.

The first tag of every DAG is its project (`digitization` or `notifications`). Use the tag filter in the DAG list to see the DAGs of one project. The other tags describe what the DAG does.

## Setup

Run these commands from the `airflow/` folder:

```bash
docker compose up --build
```

- UI: http://localhost:8080 (user `admin`, password `admin`)
- New DAGs start **paused**. Unpause a DAG before you trigger it.

### Connections

The DAGs use these Airflow connections. Create them in **Admin → Connections**, or put them in `scripts/connections.json` and the bootstrap service imports them on startup.

| Conn ID                | Type                                                                                                                                                     | Used for                                                                            |
| ---------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------- |
| `aws_s3_cern`          | Amazon Web Services (access key, secret key, `endpoint_url` in extra)                                                                                    | Reading and writing files in S3                                                     |
| `cernbox_conn`         | HTTP (login = CERN account, password)                                                                                                                    | Uploading XMLs and logs to CERNBox                                                  |
| `atom_conn`            | HTTP (host `https://archives.cern.ch`, password = AtoM REST API key)                                                                                     | Reading the descriptions and adding their digital objects (`publish_to_repository`) |
| `repository_cern_conn` | HTTP (host `https://repository.cern`, password = personal token with the `deposit:write` scope)                                                          | Creating the records on repository.cern (`publish_to_repository`)                   |
| `library_catalog_conn` | HTTP (host = catalogue URL). Same connection as the BI DAGs.                                                                                             | Reading the new items/e-items and their documents (`library_newsletter`)            |
| `newsletter_smtp`      | SMTP (host `cernmx.cern.ch`, port `25`, no login; extra `{"to": "<recipients>", "from": "<sender>", "starttls": true, "unsubscribe_url": "<optional>"}`) | Sending the newsletter (`library_newsletter`)                                       |

The catalogue backoffice API token is not in the connection: it is the `CATALOG_API_TOKEN` Variable (**Admin → Variables**), as for the BI DAGs.

Public CERNBox links (used to read the box Excel files) need no credentials.

`cernmx.cern.ch` is the CERN mail relay: it accepts emails without login from machines inside the CERN network.

---

## `validate_files_integrity`

Expected S3 layout: `<bucket>/<base_path>/BOITE_O0<n>.../*.pdf`

| Param            | Default                  | Description                                                                           |
| ---------------- | ------------------------ | ------------------------------------------------------------------------------------- |
| `data_source`    | _(required)_             | The boxes to check. See the formats below.                                            |
| `bucket`         | `digitization-dev`       | The S3 bucket.                                                                        |
| `base_path`      | `cern-archives/raw/PDF/` | The S3 prefix that contains the `BOITE_*` folders.                                    |
| `upload_reports` | `false`                  | If `true`, uploads `validation_report.json` and `s3_pdf_issues.log` to `<base_path>`. |

`data_source` formats:

| Format                          | Example                                                    | Boxes                                                            |
| ------------------------------- | ---------------------------------------------------------- | ---------------------------------------------------------------- |
| Single number                   | `42`                                                       | 42                                                               |
| Range (inclusive)               | `1..10`                                                    | 1 to 10                                                          |
| List                            | `[3, 7, 12]`                                               | 3, 7, 12                                                         |
| CERNBox URL or public link hash | `https://cernbox.cern.ch/s/AbCdEf123456` or `AbCdEf123456` | The box numbers in the names of the `.xlsx` files in that folder |

Examples:

```json
{ "data_source": "1..50" }
{ "data_source": "[101, 102]", "upload_reports": true }
{ "data_source": "AbCdEf123456", "base_path": "cern-archives/raw/PDF/" }
```

Output: the task logs show a summary with the number of valid PDFs, corrupted PDFs, and missing boxes. The full report is available in the XCom of `validate_s3_pdfs`.

---

## `match_and_export`

Expected S3 layout: `<bucket>/<base_path>/<file_type>/BOITE_O0<n>.../...`

| Param            | Default                 | Description                                                                                                 |
| ---------------- | ----------------------- | ----------------------------------------------------------------------------------------------------------- |
| `data_source`    | _(required)_            | Where to find the box `.xlsx` files: a CERNBox URL, or a folder path inside the container.                  |
| `base_paths`     | `cern-archives/raw/`    | The S3 root path. To search more than one root, separate them with commas.                                  |
| `file_types`     | `PDF,PDF_LATEX`         | The subfolders to match under each base path, separated by commas (for example `PDF`, `PDF_LATEX`, `TIFF`). |
| `bucket`         | `digitization-dev`      | The S3 bucket.                                                                                              |
| `execution_mode` | `Full Export (CERNBox)` | See the table below.                                                                                        |
| `cernbox_path`   | `xml_exports`           | The destination folder in your CERNBox (relative to your home folder).                                      |

`execution_mode` options:

| Mode                               | Matching | Mismatch log upload |         XML generation         | XML upload |
| ---------------------------------- | :------: | :-----------------: | :----------------------------: | :--------: |
| `Dry Run`                          |    ✅    |         ❌          |               ❌               |     ❌     |
| `Generate XMLs (Temp Folder Only)` |    ✅    |         ✅          | ✅ (temp folder in the worker) |     ❌     |
| `Full Export (CERNBox)`            |    ✅    |         ✅          |               ✅               |     ✅     |

`data_source` examples:

- Public share: `https://cernbox.cern.ch/s/AbCdEf123456`
- EOS path: `https://cernbox.cern.ch/files/spaces/eos/user/j/jdoe/boites` (requires `cernbox_conn`)
- Local folder: `/opt/airflow/tests/data/boites` (the path must be mounted in the container; see the `volumes` section in `docker-compose.yaml`)

Examples:

```json
{ "data_source": "https://cernbox.cern.ch/s/AbCdEf123456", "execution_mode": "Dry Run" }

{
  "data_source": "https://cernbox.cern.ch/s/AbCdEf123456",
  "base_paths": "cern-archives/raw/, cern-archives/batch2/",
  "file_types": "PDF,PDF_LATEX,TIFF",
  "cernbox_path": "xml_exports/2026-10"
}
```

Output in CERNBox (`<cernbox_path>/`):

- One XML file per box, plus `Boites_combined.xml` (Full Export only)
- `all_boites_mismatches_<run_id>.json`: the records that were not found in S3, and the S3 files that no record matched

---

## `publish_to_repository`

Expected S3 layout: `<bucket>/<prefix>/<AtoM slug>.<extension>`, one file per AtoM description. For example, `atom test/CERN-ARCH-Test-001.pdf` belongs to https://archives.cern.ch/index.php/cern-arch-test-001. Subfolders of the prefix are included.

For each file:

1. `list_s3_files`: the file name without its extension, in lowercase, is the AtoM slug (`CERN-ARCH-Test-001.pdf` → `cern-arch-test-001`). Names that are not a valid slug (letters, digits and hyphens), and slugs that appear more than once (for example `X.pdf` and `X.jpg`), are reported and skipped.
2. `publish`:
   1. Reads the AtoM description. A description that already has a digital object is skipped (`already_linked`).
   2. Looks for the repository.cern record of the description: the AtoM URL is stored in the record as a related identifier (`isdescribedby`). If there is no record, creates one with the metadata of the description (title, dates, creators, reference code, scope and content), uploads the file from S3, and submits it to the community.
   3. When the record is published, adds its file link (`https://repository.cern/records/<id>/files/<file>`) to the AtoM description as an external digital object. AtoM downloads the file to make the thumbnail.
   4. Moves the linked file to `<prefix>/<done_folder>/` (default `migrated/`), keeping its subfolders: `atom test/CERN-ARCH-Test-001.pdf` → `atom test/migrated/CERN-ARCH-Test-001.pdf`. Only `linked` files are moved; the others stay for the next run. The done folder is not read by the next runs. If the move fails, the file stays `linked` with the error, and the run fails: move it by hand.
3. `report`: prints the number of files for each status, then, for each file, its status, its CDS record and file links, its AtoM link, and where it is in S3. The run fails if a file has the status `error`, or if no file has an AtoM description (usually a wrong `atom_conn`). After 5 errors in a row (for example, repository.cern is down), the run stops and the other files are `deferred`.

The AtoM API shows the digital object of a description only if its fields are visible (**Admin → Settings → Visible elements → Digital object metadata**). If they are hidden, the DAG cannot see that a description already has a digital object before it creates the record. AtoM then refuses the link, and the file is reported as `already_linked` with the URL of the new record: delete that record on repository.cern if it is not needed.

The DAG finds the records it created through the repository.cern search, which is updated a few seconds after a write. Do not start a run while another one is running.

The hosts of `atom_conn` and `repository_cern_conn` must start with `https://`.

The community reviews every submission (`review_policy: closed`). Until a curator accepts the record, the file is `pending_review`, and AtoM is not updated. Run the DAG again after the review: the published records are found again and linked, and nothing is created twice. If the account of the token is a curator of the community, `auto_accept` accepts the submissions in the same run.

#### Settings per environment

What to configure in QA and in production (connections, Variable, deployment, first run): see [`dags/digitization_workflows/repository_publish/README.md`](dags/digitization_workflows/repository_publish/README.md).

QA and production run the same image, so the settings that depend on the environment are in the JSON Variable `publish_to_repository` (**Admin → Variables**, the `AIRFLOW_VAR_PUBLISH_TO_REPOSITORY` environment variable, or locally `scripts/variables.json`, which the bootstrap service imports). For example, for QA with the sandbox:

```json
{
  "bucket": "cern-archives-multilateralism",
  "prefix": "atom test",
  "extension": ".pdf,.jpg",
  "community": "multilateralism"
}
```

| Key              | Default       | Description                                                                                                                                 |
| ---------------- | ------------- | ------------------------------------------------------------------------------------------------------------------------------------------- |
| `bucket`         | _(required)_  | The S3 bucket.                                                                                                                              |
| `prefix`         | _(required)_  | The S3 folder with the files.                                                                                                               |
| `community`      | _(required)_  | The repository.cern community. It must exist on the host of `repository_cern_conn` (the sandbox and production have different communities). |
| `extension`      | `.pdf`        | Comma-separated extensions of the files, for example `.pdf,.jpg`.                                                                           |
| `resource_type`  | `publication` | The resource type of the new records (an ID of https://repository.cern/api/vocabularies/resourcetypes).                                     |
| `done_folder`    | `migrated`    | Subfolder of `prefix` where the linked files are moved. `""`: the files stay in place.                                                      |
| `reports_folder` | `reports`     | Subfolder of `prefix` for the reports of `upload_report`. Its files are never published. `""`: the reports go to `prefix` itself.           |
| `max_deposits`   | `50`          | Maximum number of new records per run. The files after that are `deferred` to the next run. Files that only need a link do not count.       |

The trigger form shows the values of the Variable, read when Airflow parses the DAG file: after a change of the Variable, the form shows the new values after the next parse (or `airflow dags reserialize`). A trigger param overrides the Variable for one run; an empty param uses the Variable at run time. Without a value for `bucket`, `prefix` or `community`, the run fails before it reads S3. Unknown keys in the Variable (for example a typo) also fail the run. The `list_s3_files` log shows the settings of the run.

For each environment, set:

|                                  | Local / QA                            | Production                                  |
| -------------------------------- | ------------------------------------- | ------------------------------------------- |
| `repository_cern_conn` host      | `https://sandbox-cds-rdm.web.cern.ch` | `https://repository.cern`                   |
| `repository_cern_conn` token     | A sandbox token                       | A production token                          |
| `publish_to_repository` Variable | The test bucket, folder and community | The production bucket, folder and community |

Other params:

| Param                | Default   | Description                                                                                                                                                                                                                                                                                                                                                          |
| -------------------- | --------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `slugs`              | _(empty)_ | Comma-separated slugs: only these files are processed. File names also work (`CERN-ARCH-Test-002.jpg` → `cern-arch-test-002`).                                                                                                                                                                                                                                       |
| `auto_accept`        | `false`   | Accepts the community submissions (the token must be a curator of the community).                                                                                                                                                                                                                                                                                    |
| `execution_mode`     | `Dry Run` | See the table below.                                                                                                                                                                                                                                                                                                                                                 |
| `upload_report`      | `false`   | If `true`, uploads `publish_report_<run_id>.json` to `<prefix>/<reports_folder>/` (default `<prefix>/reports/`).                                                                                                                                                                                                                                                     |
| `reprocess_migrated` | `false`   | Also processes the files of `slugs` that are in the done folder. Needs `slugs`. If a slug also has a file outside the done folder, that new file is used. Files that are already in the done folder stay there.                                                                                                                                                      |
| `refresh_metadata`   | `false`   | Updates the published records that already existed with the current AtoM metadata, even if AtoM is already linked. The fields that the DAG maps (title, dates, creators, description, publisher, resource type) are replaced; the others, for example subjects added by a curator, are kept. Same record and file, no new review. `Dry Run` reports `would_refresh`. |
| `replace_file`       | `false`   | Publishes a **new version** of the published records with the S3 file and the current AtoM metadata (repository.cern locks the files of a published record). If the file is the same as the published one, no version is created and only the metadata is refreshed. Needs `slugs`. `Dry Run` reports `would_replace_file`.                                          |

#### Updating a file that is already migrated

The AtoM API cannot delete or replace a digital object, so the DAG never changes a link that is already in AtoM.

- **Link it again** (for example, the link was made before a fix): delete the digital object in AtoM (**Edit digital object → Delete**), then trigger with `slugs` = the slug, `reprocess_migrated` = `true`, and `Deposit and link`. The DAG finds the published record and links it again.
- **Refresh the metadata on repository.cern** after the AtoM description changed: trigger with `slugs`, `reprocess_migrated` = `true`, `refresh_metadata` = `true`, and `Deposit only` (or `Deposit and link`). Try it with `Dry Run` first.
- **Replace the file** (for example a better scan), with the current metadata:
  1. Upload the new file to `<prefix>/` with the same name (or overwrite the one in the done folder).
  2. Delete the digital object of the description in AtoM, so that AtoM can link the new version.
  3. Trigger with `slugs`, `reprocess_migrated` = `true`, `replace_file` = `true`, and `Deposit and link` (`Dry Run` first).

  The new version gets a new record id and file link; the previous version stays public. If AtoM still has a digital object, the new version is published, but AtoM keeps the link to the previous one (the task log warns).

The report shows `record_refreshed`, `record_new_version` (or `record_would_refresh`, `record_would_replace_file`) for each updated record.

`execution_mode` options:

| Mode               | Reads S3, AtoM and repository.cern | Creates and submits records | Updates AtoM |
| ------------------ | :--------------------------------: | :-------------------------: | :----------: |
| `Dry Run`          |                 ✅                 |             ❌              |      ❌      |
| `Deposit only`     |                 ✅                 |             ✅              |      ❌      |
| `Deposit and link` |                 ✅                 |             ✅              |      ✅      |

Statuses:

| Status                                        | Meaning                                                                                                                                                                                  |
| --------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `linked`                                      | The record is published and AtoM has the link.                                                                                                                                           |
| `already_linked`                              | The AtoM description already has a digital object: AtoM was not changed.                                                                                                                 |
| `published`                                   | The record is published, but AtoM was not updated (`Dry Run` or `Deposit only`).                                                                                                         |
| `pending_review`                              | The record waits for the review of the community. Also when `auto_accept` is refused because the account of the token is not a curator: the log warns, and a curator accepts the record. |
| `review_declined`                             | The community declined the record, or the review expired. Fix it on repository.cern.                                                                                                     |
| `would_deposit`                               | `Dry Run`: a record would be created.                                                                                                                                                    |
| `deferred`                                    | `max_deposits` was reached: the file is processed in the next run.                                                                                                                       |
| `no_atom_description`                         | There is no AtoM description with this slug.                                                                                                                                             |
| `no_date`                                     | The AtoM description has no date: repository.cern requires a publication date.                                                                                                           |
| `invalid_slug`, `duplicate_slug`, `not_in_s3` | The S3 file name is not a valid slug, the slug is in several files, or a slug of `slugs` has no file.                                                                                    |
| `error`                                       | Unexpected error, see `error` in the report and the task logs.                                                                                                                           |

Examples:

```json
{ "execution_mode": "Dry Run" }
{ "slugs": "cern-arch-test-001", "execution_mode": "Deposit only" }
{ "prefix": "atom test/batch-2", "execution_mode": "Deposit and link", "upload_report": true }
{ "slugs": "cern-arch-test-002", "reprocess_migrated": true, "refresh_metadata": true, "execution_mode": "Deposit only" }
```

---

## `library_newsletter`

Runs every Monday at 08:00 Geneva time (summer and winter) for the previous Monday–Sunday week. Code: `dags/notifications/`.

How it works:

1. `get_newsletter_week`: picks the week: `week_start` if given, otherwise the full week before the day the run was scheduled or triggered. A scheduled run that is retried or cleared later still sends its own week.
2. `fetch_new_documents`: gets the `document_pid` of the items and e-items created that week from the backoffice API (`/api/items`, `/api/eitems`), then their metadata from the public API (`/api/literature`). The creation date of the item is used, not of the document: a physical book's document is created when it is ordered, and its item when it arrives. Restricted documents are not returned by the public API.
3. `build_newsletter`: groups the documents by topic from their subjects (UDC/Dewey prefixes, see `TOPICS` in `config.py`), and renders one email. A document with several topics appears under each of them. Documents without a matching topic go to `Other`. Every document is listed (as in the previous service).
4. `send_newsletter`: sends the email, depending on `execution_mode`. Nothing is sent if there are no new documents.

Each newsletter has a fixed `Message-ID` built from its week, for example `<library-newsletter-2026-11-09@cern.ch>`. After the SMTP server accepts the email, the `Message-ID` and the send time are saved in the `library_newsletter_sent` Variable (Admin → Variables). Before sending, the task checks this Variable and skips a newsletter that was already sent, so a retry or a cleared task does not send it twice. Entries older than 60 days (`SENT_HISTORY_DAYS` in `config.py`) are removed at each send. To send a week again, trigger the DAG with `force_resend`: the email gets its own `Message-ID` (for example `<library-newsletter-2026-11-09.resend-20261118T114503@cern.ch>`), so a retry of the resend is also detected. Dry Run does not use the Variable.

| Param            | Default   | Description                                                                       |
| ---------------- | --------- | --------------------------------------------------------------------------------- |
| `execution_mode` | `Send`    | `Dry Run`: the email is only printed in the task logs. `Send`: the email is sent. |
| `week_start`     | _(empty)_ | Monday (`YYYY-MM-DD`) of the week to send, for example to resend a past week.     |
| `force_resend`   | `false`   | Sends the newsletter even if it was already sent.                                 |

The recipients are not a param: they are the `to` extra of the `newsletter_smtp` connection (comma-separated), so each environment sends to its own list:

| Environment | `to`                                                                            |
| ----------- | ------------------------------------------------------------------------------- |
| QA (dev)    | A temporary test group, currently `library-newsletter-test@cern.ch` (see below) |
| Production  | `library-newsletter@cern.ch`, the real newsletter list                          |

To add an unsubscribe link at the end of the email, set the `unsubscribe_url` extra of `newsletter_smtp`, for example to the page of the list in [CERN Groups](https://groups-portal.web.cern.ch). Without it, the email has no unsubscribe link.

Scheduled runs use `Send`. To try the DAG without an SMTP connection, trigger it with `Dry Run`.

To test `Send` without emailing the subscribers of `library-newsletter@cern.ch`, send to a test group instead:

1. Create a test group in [CERN Groups](https://groups-portal.web.cern.ch) with the people who need to check the email. Test groups are temporary: CERN Groups removes them after some time, so the QA group may need to be created again.
2. Set its address as the `to` extra of `newsletter_smtp`.
3. Trigger the DAG with `Send`.

#### Catalogue: sandbox or production

The DAG reads the catalogue of the `library_catalog_conn` connection, shared with the BI DAGs:

| Environment   | Host of `library_catalog_conn`                   |
| ------------- | ------------------------------------------------ |
| Local and dev | `https://sandbox-cds-ils.web.cern.ch/` (sandbox) |
| Production    | `https://catalogue.library.cern`                 |

The sandbox gets almost no new documents, so locally and in dev the newsletter is usually empty or lists test records. The task logs show a warning when the catalogue is not the production one.

To test the newsletter with real data, locally or in dev, change it **temporarily**:

1. In **Admin → Connections**, set the host of `library_catalog_conn` to `https://catalogue.library.cern`.
2. In **Admin → Variables**, set `CATALOG_API_TOKEN` to a **production** token. A sandbox token does not work on the production catalogue.
3. Trigger the DAG with `Dry Run`.
4. Put back the sandbox host and token. In dev, the BI DAGs use the same connection and Variable, so they also read production until then.

The email layout is in the Jinja2 templates `dags/notifications/templates/newsletter.html` and `newsletter.txt` (plain text version). To add a topic, add a line to `TOPICS` in `dags/notifications/config.py`. Two optional settings are also there, both off by default: a maximum number of documents per topic (`MAX_RESULTS_PER_TOPIC`, the rest is shown as `+ N more`) and a filter on the publication year (`PUBLICATION_YEARS_WINDOW`).

Examples:

```json
{ "execution_mode": "Dry Run", "week_start": "2026-11-02" }
{ "execution_mode": "Send", "week_start": "2026-11-02" }
{ "execution_mode": "Send", "week_start": "2026-11-02", "force_resend": true }
```

### Tests

From the repository root, with the dev dependencies installed (Airflow and `pytest`):

```bash
python -m pytest airflow/tests
```
