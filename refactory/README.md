# CERN Digitization

Validate PDF files, match Boite Excel inventory records against S3 files, and export the results to XML (FFT) for CDS upload. These tools can be executed either via a Command Line Interface (CLI) or orchestrated through Apache Airflow pipelines.

## Structure

- `cli.py` - click CLI exposing the main workflows:
  - `validate-files-integrity`
  - `match-and-export`
- `storage_connection.py` - storage provider abstraction:
  - `S3Provider` for S3.
  - `CernboxProvider` for public/authenticated CERNBox access.
- `check_files/main.py` - validation pipeline used by `validate-files-integrity`.
- `file_import/boite_matcher.py` - Boite-to-S3 matcher implementation used by `match-and-export`.
- `file_import/xml_exporter.py` - XML generator (FFT) used for CDS batch uploads.

## 1. CLI Usage (Local Execution)

Run the refactory CLI from the repository root using Poetry:

```bash
poetry run digitization_v2 --help
```

### Command: validate-files-integrity
Use this command to check the Boite inventory against the PDF validation pipeline locally.

```bash
poetry run digitization_v2 validate-files-integrity \
  -d "[122,123]" \
  -u \
  -b digitization-dev
```

**Options:**
- `-d, --data-source` — Boite inventory source. Supports a CERNBox hash, range (`1..10`), or list (`[1,2]`).
- `-u, --upload-reports` — upload validation reports back to storage.
- `-b, --bucket` — S3 bucket name (default: `digitization-dev`).
- `-p, --base-path` — Base S3 path (default: `cern-archives/raw/PDF/`).

---

### Command: match-and-export
Use this command to match Boite Excel filenames with S3 objects, write structured JSON outputs to the local results directory, and optionally generate/upload XML files.

```bash
poetry run digitization_v2 match-and-export \
  -d "[https://cernbox.cern.ch/s/](https://cernbox.cern.ch/s/){hash}" \
  -p "cern-archives/raw/CORRECTIONS_2,cern-archives/raw/" \
  -o ./results \
  -f PDF,PDF_LATEX \
  -b digitization-dev \
  -r \
  -x \
  -c
```

**Options:**
- `-d, --data-source` — local directory or CERNBox URL containing `.xlsx` Boite files.
- `-p, --base-paths` — Comma-separated base S3 paths. Order defines priority (default: `cern-archives/raw/`).
- `-o, --output-path` — output directory for local JSON/XML results (default: `./results`).
- `-f, --file-types` — comma-separated list of file types to match (default: `PDF,PDF_LATEX`).
- `-b, --bucket` — S3 bucket name (default: `digitization-dev`).
- `-r, --report` — Display detailed run summary metrics and listed missing records in the console.
- `--dry-run` — Stop script execution after the matching phase. No XML generation or uploads will occur.
- `-x, --generate-xml` — Generate XML files (FFT) for CDS upload.
- `-c, --upload-cernbox` — Upload the generated XML files to CERNBox.
- `--cernbox-path` — Target folder inside CERNBox for XML uploads (default: `xml_exports`).

---

## 2. Airflow Usage (Orchestrated Execution)
### 🐳 Running Airflow via Docker

Run Docker Compose from the root directory containing your configuration deployment files:
```bash
docker compose up -d
```

### Verify DAG Mounting
Ensure that your local repository or specific domain directories (`common/`, `check_files/`, `file_import/`) are mapped as volumes inside your `docker-compose.yaml` to point to `/opt/airflow/dags/`. This guarantees changes in code are picked up by the Scheduler automatically.

### Accessing the Control Panel
Once the startup scripts finish loading the bundles, open your browser and access the Web UI:
* **URL**: `http://localhost:8080`
* **Default Credentials**: `airflow` / `airflow` (or your infrastructure environment profile keys).

When deployed or running inside an Airflow environment, the execution parameters are managed via the Airflow Web UI configuration panel (*Trigger DAG w/ config*).

Unlike the local CLI execution, the Airflow pipelines follow a strict cloud-native pattern:
- **In-Memory Operations**: Processing is performed inside ephemeral temporary storage directories (`/tmp`).
- **Server Disk Integrity**: Intermediate files are automatically purged at task completion, meaning the local `./results` folder is bypassed entirely.
- **Unified Tracing**: Output is routed via standard Python `logging` for server visibility.

### DAG: match_and_export
Coordinates index validation and automates delivery based on the selected execution mode dropdown parameters:
1. `Dry Run`: Executes matching logic and uploads system audit logs back to S3. Stops before XML generation.
2. `Generate XMLs (Temp Folder Only)`: Runs full match pipelines and instantiates the `XMLExporter` engine locally to validate formatting. Deletes artifacts before network transfer.
3. `Full Export (CERNBox)`: Runs the entire pipeline, compiles individual and combined XML data arrays, and syncs them directly to the targeted folder inside CERNBox.

### DAG: validate_files_integrity
Scans designated storage paths in S3, checks the binary layout header of every PDF asset, and uploads an execution overview report.

---

## Authentication & Credentials

Depending on how you run the tools, credentials must be provided either via Local Environment Variables (for CLI) or registered via the Airflow Metadata Database (for Airflow).

### 1. AWS Authentication

* **For CLI**: Configure your shell using standard variables or local files:
  - `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`
  - Local configuration paths: `~/.aws/credentials` or `~/.aws/config`
* **For Airflow**: Register an **Amazon Web Services** Connection ID named `aws_s3_cern` under `Admin -> Connections` (or ensure your Airflow Worker node inherits an appropriate IAM role configuration).

> Note: `S3Provider` targets the default endpoint `https://s3.cern.ch` as defined in `storage_connection.py`.

### 2. CERNBox Authentication
`CernboxProvider` communicates over WebDAV APIs.

* **For CLI**: Set the following variables before invoking the command root:
  ```bash
  export CERNBOX_USER="your_username"
  export CERNBOX_PASSWORD="your_password"
  ```
* **For Airflow**: Register an **HTTP** Connection ID named `cernbox_conn` under `Admin -> Connections`:
  - **Host**: `https://api.cernbox.cern.ch`
  - **Login**: Your CERN computing ID
  - **Password**: Your main password or an infrastructure-generated App Password.
---

## Dependencies Management

This project manages environment environments differently depending on your target execution context:

* **Local Development (CLI)**: Managed via **Poetry**. The core workflow tools and deterministic lockfile configurations are defined inside `pyproject.toml`.
* **Airflow Deployment (Docker)**: Managed via **`requirements.txt`**. When building or initializing the infrastructure via containerized stacks, the ecosystem installs and locks the application environment layer using this file.

---
