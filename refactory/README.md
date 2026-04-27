# refactory

This directory contains tools for validating PDF files, matching Boite Excel inventory records against S3 files, and optionally exporting the results to XML (FFT) for CDS upload.

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

## CLI usage

Run the refactory CLI from the repository root:

```bash
poetry run digitization_v2 --help
```

The available commands are:

- `validate-files-integrity` — validate PDF integrity and inventory alignment.
- `match-and-export` — match Boite Excel records against S3 files, generate JSON outputs, and optionally export/upload XMLs.

---

## 1. Validate files integrity

Use this command to check the Boite inventory against the PDF validation pipeline.

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

This command runs the validation pipeline and generates logs such as `s3_pdf_issues.log`.

---

## 2. Match and Export (Boite-to-S3)

Use this command to match Boite Excel filenames with S3 objects, write structured JSON outputs, and optionally generate and upload XML files for CDS.

```bash
poetry run digitization_v2 match-and-export \
  -d "[https://cernbox.cern.ch/s/](https://cernbox.cern.ch/s/){hash}" \
  -p "cern-archives/raw/" \
  -o ./results \
  -f PDF,PDF_LATEX \
  -b digitization-dev \
  -x \
  -c
```

**Options:**

- `-d, --data-source` — local directory or CERNBox URL containing `.xlsx` Boite files.
- `-p, --base-path` — Base S3 path (default: `cern-archives/raw/`).
- `-o, --output-path` — output directory for JSON/XML results (default: `./results`).
- `-f, --file-types` — comma-separated list of file types to match (default: `PDF,PDF_LATEX`).
- `-b, --bucket` — S3 bucket name (default: `digitization-dev`).
- `-x, --generate-xml` — Generate XML files (FFT) for CDS upload.
- `-c, --upload-cernbox` — Upload the generated XML files to CERNBox.
- `--cernbox-path` — Target folder inside CERNBox for XML uploads (default: `xml_exports`).

### Matcher & Export behavior

The `match-and-export` flow:

1. **Downloads** `.xlsx` Boite files from CERNBox if a URL is provided.
2. **Reads** each Boite file and extracts the record ID and filename columns.
3. **Searches** S3 under `<BASE_PATH><TYPE>/<BOITE>/`.
4. **Matches** filenames case-insensitively. Supports both flat and subfolder layouts:
   - *Flat:* `raw/PDF_LATEX/BOITE_O0125/ISR-LEP-RF-GG-ps.pdf`
   - *Nested:* `raw/PDF/BOITE_O0125/LEP-RF-SH-ps/LEP-RF-SH-ps.pdf`
5. **Generates** unified mismatch logs in JSON format for missing Boite rows and extra S3 files.
6. **(Optional) Exports** matching records to XML files if the `-x` flag is used.
7. **(Optional) Uploads** the generated XMLs to a specified path in CERNBox if the `-c` flag is used.

---

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
- `click`

---

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

---

## CERNBox Authentication

`CernboxProvider` reads optional credentials from environment variables:

- `CERNBOX_USER`
- `CERNBOX_PASSWORD`

### Example environment variables for CERNBox

```bash
export CERNBOX_USER="your_username"
export CERNBOX_PASSWORD="your_password"
```

---

## Notes

- `file_import/boite_matcher.py` is the primary matcher used by `match-and-export`.
- `test_connections.py` can be used to verify storage connectivity before running either workflow.
- Use `poetry run digitization_v2 --help` to verify command names and options at runtime.
