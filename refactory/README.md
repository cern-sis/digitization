# refactory

This directory contains scripts and helpers for validating PDF files in an S3 bucket using an inventory of Excel files hosted on CERNBox.

## Structure

- `main.py` - main script that validates PDFs using the CERNBox inventory.
- `storage_connection.py` - storage provider abstraction:
  - `S3Provider` for S3.
  - `CernboxProvider` for public CERNBox access.
- `validate_pdf.py` - validates PDFs locally with `is_pdf_valid(file_path)`.
- `test_connections.py` - testing/connection experiment script.

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

> If the project is managed with Poetry, `requirements.txt` is not required.

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

## Usage with Poetry

Run the refactored CLI via Poetry:

```bash
poetry run digitization_v2 --help
```

The current command for PDF validation is `validade-files-integrity`.

### Example

```bash
poetry run digitization_v2 check-integrity -s "[122,123]" -u
```

Parameters:

- `-i, --inventory-source`: Inventory source. Supports CERNBOX Hash, range (`1..10`), or list (`[1,2]`).
- `-u, --upload-reports`: Flag to upload validation reports back to the storage provider.
- `-b, --bucket`: S3 bucket name (default: `digitization-dev`).

### Example without upload

```bash
poetry run digitization_v2 check-integrity -s "[122,123]"
```

## Expected output

The CLI generates the same validation reports as the core pipeline:

- a text log file such as `s3_pdf_issues.log`
- a structured JSON report with valid, corrupted, and missing file details

If `-u` is provided, the reports will be uploaded back to the configured storage provider.

## Additional notes

- `CernboxProvider` reads optional credentials from environment variables:
  - `CERNBOX_USER`
  - `CERNBOX_PASSWORD`

### Example environment variables for Cernbox

```bash
export CERNBOX_USER="your_username"
export CERNBOX_PASSWORD="your_password"
```

- You may still pass `account` and `password` directly to `CernboxProvider` if preferred.
- Use `test_connections.py` to verify connections before running the main pipeline.
