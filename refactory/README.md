# refactory

This directory contains tools for validating PDF files and matching Boite Excel inventory records against S3 files.

## Structure

- `cli.py` - click CLI exposing the main workflows:
  - `validate-files-integrity`
  - `file-match`
- `storage_connection.py` - storage provider abstraction:
  - `S3Provider` for S3.
  - `CernboxProvider` for public CERNBox access.
- `check_files/main.py` - validation pipeline used by `validate-files-integrity`.
- `file_import/refactory_matcher.py` - Boite-to-S3 matcher implementation used by `file-match`.
- `file_import/boite_matcher.py` - additional matcher implementation and helpers.

## CLI usage

Run the refactory CLI from the repository root:

```bash
poetry run digitization_v2 --help
```

The available commands are:

- `validate-files-integrity` — validate PDF integrity and inventory alignment.
- `file-match` — match Boite Excel records against S3 files and generate JSON outputs.

## 1. Validate files integrity

Use this command to check the Boite inventory against the PDF validation pipeline.

```bash
poetry run digitization_v2 validate-files-integrity \
  -d "[122,123]" \
  -u \
  -b digitization-dev
```

Options:

- `-d, --data-source` — Boite inventory source. Supports a CERNBox hash, range (`1..10`), or list (`[1,2]`).
- `-u, --upload-reports` — upload validation reports back to storage.
- `-b, --bucket` — S3 bucket name (default: `digitization-dev`).

This command runs the validation pipeline and generates logs such as `s3_pdf_issues.log`.

## 2. Boite-to-S3 file matching

Use this command to match Boite Excel filenames with S3 objects and write structured JSON output.

```bash
poetry run digitization_v2 file-match \
  -d "https://cernbox.cern.ch/s/{hash}" \
  -o ./match_results \
  -f PDF,PDF_LATEX \
  -b digitization-dev
```

Options:

- `-d, --data-source` — local directory or CERNBox URL containing `.xlsx` Boite files.
- `-o, --output-path` — output directory for JSON results (default: `./match_results`).
- `-f, --file-types` — comma-separated list of file types to match (default: `PDF,PDF_LATEX`).
- `-b, --bucket` — S3 bucket name (default: `digitization-dev`).

### Matcher behavior

The `file-match` flow:

- downloads `.xlsx` Boite files from CERNBox if a URL is provided.
- reads each Boite file and extracts the record ID and filename columns.
- searches S3 under `raw/<TYPE>/<BOITE>/`.
- matches filenames case-insensitively.
- supports both flat and subfolder layouts:
  - flat: `raw/PDF_LATEX/BOITE_O0125/ISR-LEP-RF-GG-ps.pdf`
  - nested: `raw/PDF/BOITE_O0125/LEP-RF-SH-ps/LEP-RF-SH-ps.pdf`
- writes unified mismatch logs in JSON format for missing Boite rows and extra S3 files.

## Dependencies

This project uses Poetry to manage dependencies. The required libraries are listed in `pyproject.toml`.

### Install dependencies with Poetry

```bash
poetry install
```

### Main dependencies

- `boto3`
- `requests`
- `pypdf`

## AWS Authentication

`S3Provider` uses `boto3`. Configure credentials using environment variables or the default AWS config files:

- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`

### Example environment variables

```bash
export ACCESS_KEY="YOUR_ACCESS_KEY"
export SECRET_KEY="YOUR_SECRET_KEY"
```

### Supported alternatives

- `~/.aws/credentials`
- `~/.aws/config`
- IAM role attached to an instance/container

> `S3Provider` also supports the default endpoint `https://s3.cern.ch`, configured in `storage_connection.py`.

## CERNBox Authentication

`CernboxProvider` reads optional credentials from environment variables:

- `CERNBOX_USER`
- `CERNBOX_PASSWORD`

### Example environment variables for CERNBox

```bash
export CERNBOX_USER="your_username"
export CERNBOX_PASSWORD="your_password"
```

## Notes

- `file_import/refactory_matcher.py` is the primary matcher used by `file-match`.
- `test_connections.py` can be used to verify storage connectivity before running either workflow.
- Use `poetry run digitization_v2 --help` to verify command names and options at runtime.
