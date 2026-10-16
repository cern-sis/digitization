# Digitization – Airflow

Airflow DAGs for the digitization workflows:

| DAG | What it does |
| --- | --- |
| `validate_files_integrity` | Checks that the PDFs of the selected boxes in S3 can be opened (not corrupted). |
| `match_and_export` | Matches the records in the box Excel files (`BOITE_O0<n>.xlsx`) with the files in S3, then builds the MARCXML files. |

## Setup

Run these commands from the `airflow/` folder:

```bash
docker compose up --build
```

- UI: http://localhost:8080 (user `admin`, password `admin`)
- New DAGs start **paused**. Unpause a DAG before you trigger it.

### Connections

The DAGs use two Airflow connections. Create them in **Admin → Connections**, or put them in `scripts/connections.json` and the bootstrap service imports them on startup.

| Conn ID | Type | Used for |
| --- | --- | --- |
| `aws_s3_cern` | Amazon Web Services (access key, secret key, `endpoint_url` in extra) | Reading and writing files in S3 |
| `cernbox_conn` | HTTP (login = CERN account, password) | Uploading XMLs and logs to CERNBox |

Public CERNBox links (used to read the box Excel files) need no credentials.

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
