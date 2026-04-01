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

Run the main script via Poetry:

```bash
poetry run python refactory/main.py <target_excel_hash> <upload_reports>
```

Parameters:

- `target_excel_hash`: public CERNBox hash containing the inventory Excel files.
- `upload_reports`: `0` to skip report upload, `1` to upload generated reports back to the S3 bucket. 

### Example

```bash
poetry run python refactory/main.py QslvWRIPsBcDAOK 0
```

## Expected output

The script generates:

- `s3_pdf_issues.log` - text log with valid and corrupted files.
- `s3_pdf_issues.json` - structured report with metadata, statistics, and file lists.

If `upload_reports=1`, the reports are also uploaded back to the S3 bucket.

## Additional notes

- If CERNBox upload requires authentication, provide `account` and `password` to `CernboxProvider`.
- Use `test_connections.py` to verify connections before running the main pipeline.
