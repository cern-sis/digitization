# Digitization – Airflow

Airflow DAGs for the digitization workflows:

| DAG | What it does |
| --- | --- |
| `validate_files_integrity` | Checks that the PDFs of the selected boxes in S3 can be opened (not corrupted). |
| `match_and_export` | Matches the records in the box Excel files (`BOITE_O0<n>.xlsx`) with the files in S3, then builds the MARCXML files. |

Airflow DAGs for the library notifications:

| DAG | What it does |
| --- | --- |
| `library_newsletter` | Every Monday, emails the books/e-books added to the library catalogue in the previous week, grouped by topic. |

### Project layout

Each project has its own folder in `dags/`:

```
dags/
├── digitization_workflows/   # digitization project
│   ├── common/               #   storage providers (S3, CERNBox) and helpers
│   ├── check_files/          #   validate_files_integrity
│   └── file_import/          #   match_and_export
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

| Conn ID | Type | Used for |
| --- | --- | --- |
| `aws_s3_cern` | Amazon Web Services (access key, secret key, `endpoint_url` in extra) | Reading and writing files in S3 |
| `cernbox_conn` | HTTP (login = CERN account, password) | Uploading XMLs and logs to CERNBox |
| `library_catalog_conn` | HTTP (host = catalogue URL). Same connection as the BI DAGs. | Reading the new items/e-items and their documents (`library_newsletter`) |
| `newsletter_smtp` | SMTP (host `cernmx.cern.ch`, port `25`, no login; extra `{"to": "<recipients>", "from": "<sender>", "starttls": true}`) | Sending the newsletter (`library_newsletter`) |

The catalogue backoffice API token is not in the connection: it is the `CATALOG_API_TOKEN` Variable (**Admin → Variables**), as for the BI DAGs.

Public CERNBox links (used to read the box Excel files) need no credentials.

`cernmx.cern.ch` is the CERN mail relay: it accepts emails without login from machines inside the CERN network.

---

## `validate_files_integrity`

Expected S3 layout: `<bucket>/<base_path>/BOITE_O0<n>.../*.pdf`

| Param | Default | Description |
| --- | --- | --- |
| `data_source` | *(required)* | The boxes to check. See the formats below. |
| `bucket` | `digitization-dev` | The S3 bucket. |
| `base_path` | `cern-archives/raw/PDF/` | The S3 prefix that contains the `BOITE_*` folders. |
| `upload_reports` | `false` | If `true`, uploads `validation_report.json` and `s3_pdf_issues.log` to `<base_path>`. |

`data_source` formats:

| Format | Example | Boxes |
| --- | --- | --- |
| Single number | `42` | 42 |
| Range (inclusive) | `1..10` | 1 to 10 |
| List | `[3, 7, 12]` | 3, 7, 12 |
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

| Param | Default | Description |
| --- | --- | --- |
| `data_source` | *(required)* | Where to find the box `.xlsx` files: a CERNBox URL, or a folder path inside the container. |
| `base_paths` | `cern-archives/raw/` | The S3 root path. To search more than one root, separate them with commas. |
| `file_types` | `PDF,PDF_LATEX` | The subfolders to match under each base path, separated by commas (for example `PDF`, `PDF_LATEX`, `TIFF`). |
| `bucket` | `digitization-dev` | The S3 bucket. |
| `execution_mode` | `Full Export (CERNBox)` | See the table below. |
| `cernbox_path` | `xml_exports` | The destination folder in your CERNBox (relative to your home folder). |

`execution_mode` options:

| Mode | Matching | Mismatch log upload | XML generation | XML upload |
| --- | :-: | :-: | :-: | :-: |
| `Dry Run` | ✅ | ❌ | ❌ | ❌ |
| `Generate XMLs (Temp Folder Only)` | ✅ | ✅ | ✅ (temp folder in the worker) | ❌ |
| `Full Export (CERNBox)` | ✅ | ✅ | ✅ | ✅ |

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

## `library_newsletter`

Runs every Monday at 08:00 Geneva time (summer and winter) for the previous Monday–Sunday week. Code: `dags/notifications/`.

How it works:

1. `get_newsletter_week`: picks the week: `week_start` if given, otherwise the full week before the day the run was scheduled or triggered. A scheduled run that is retried or cleared later still sends its own week.
2. `fetch_new_documents`: gets the `document_pid` of the items and e-items created that week from the backoffice API (`/api/items`, `/api/eitems`), then their metadata from the public API (`/api/literature`). The creation date of the item is used, not of the document: a physical book's document is created when it is ordered, and its item when it arrives. Restricted documents are not returned by the public API.
3. `build_newsletter`: groups the documents by topic from their subjects (UDC/Dewey prefixes, see `TOPICS` in `config.py`), and renders one email. A document with several topics appears under each of them. Documents without a matching topic go to `Other`. Every document is listed (as in the previous service).
4. `send_newsletter`: sends the email, depending on `execution_mode`. Nothing is sent if there are no new documents.

Each newsletter has a fixed `Message-ID` built from its week, for example `<library-newsletter-2026-11-09@cern.ch>`. After the SMTP server accepts the email, the `Message-ID` and the send time are saved in the `library_newsletter_sent` Variable (Admin → Variables). Before sending, the task checks this Variable and skips a newsletter that was already sent, so a retry or a cleared task does not send it twice. Entries older than 60 days (`SENT_HISTORY_DAYS` in `config.py`) are removed at each send. To send a week again, trigger the DAG with `force_resend`: the email gets its own `Message-ID` (for example `<library-newsletter-2026-11-09.resend-20261118T114503@cern.ch>`), so a retry of the resend is also detected. Dry Run does not use the Variable.

| Param | Default | Description |
| --- | --- | --- |
| `execution_mode` | `Send` | `Dry Run`: the email is only printed in the task logs. `Send`: the email is sent. |
| `week_start` | *(empty)* | Monday (`YYYY-MM-DD`) of the week to send, for example to resend a past week. |
| `force_resend` | `false` | Sends the newsletter even if it was already sent. |

The recipients are not a param: they are the `to` extra of the `newsletter_smtp` connection (comma-separated), so each environment sends to its own list:

| Environment | `to` |
| --- | --- |
| QA | `test-library-newsletter@cern.ch` |
| Production | `library-newsletter@cern.ch` |

Scheduled runs use `Send`. To try the DAG locally without an SMTP connection, trigger it with `Dry Run`.

#### Catalogue: sandbox or production

The DAG reads the catalogue of the `library_catalog_conn` connection, shared with the BI DAGs:

| Environment | Host of `library_catalog_conn` |
| --- | --- |
| Local and dev | `https://sandbox-cds-ils.web.cern.ch/` (sandbox) |
| Production | `https://catalogue.library.cern` |

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
