import tempfile
import re
import os
import sys
import json
from typing import Union
from refactory.storage_connection import StorageProvider, S3Provider, CernboxProvider
from .utils import validate_pdf


def run_validation_pipeline(
    provider: StorageProvider,
    base_path: str,
    log_file: str,
    data_source: Union[str, list[int]],
    upload_reports: bool = False,

):
    """Navigates directories, validates files, and logs files status."""
    target_box_numbers = set()
    if isinstance(data_source, str):
        data_source_provider = CernboxProvider(data_source)
        excel_files = data_source_provider.list_files("", '.xlsx')

        for file_path in excel_files:
            filename = file_path.split(".")[0]

            match = re.search(r"(?i:BOITE)[\-_]O0(\d+)(-\w+)?", filename)

            if match:
                target_box_numbers.add(int(match.group(1)))
    elif isinstance(data_source, list):
        target_box_numbers = set(data_source)

    print(f"Excel files: {len(target_box_numbers)} boxes to check.")

    print(f"Folders in: {base_path}")
    folders = provider.list_folders(base_path)

    if not folders:
        print("No folders found in this path.")
        return

    found_and_valid_boxes = set()
    corrupted_files = []
    valid_files = []

    print("Starting validation...")

    for folder in folders:
        match = re.search(r"(?i:BOITE)[\-_]O0(\d+)(-\w+)?", folder)
        if not match:
            continue

        box_num = int(match.group(1))
        if box_num not in target_box_numbers:
            continue
        print(f"Processing target Box: {match.group(1) + (match.group(2) or '')}")

        pdf_files = provider.list_files(folder, 'PDF')

        if not pdf_files:
            print(f"⚠️ EMPTY FOLDER: {folder}")
            continue

        found_and_valid_boxes.add(box_num)

        for pdf_path in pdf_files:
            with tempfile.NamedTemporaryFile(delete=True) as tmp:
                provider.download_to_temp(pdf_path, tmp.name)

                if validate_pdf(tmp.name):
                    valid_files.append(pdf_path)
                    print(f"  ✅ {pdf_path}")
                else:
                    print(f"  ❌ CORRUPTED: {pdf_path}")
                    corrupted_files.append(pdf_path)
    missing_boxes = target_box_numbers - found_and_valid_boxes

    if missing_boxes:
        print("\n Empty target boxes:")
        for box in sorted(missing_boxes):
            print(
                f"  -> BOITE_O0{box}"
            )

    with open(log_file, "w", encoding="utf-8") as log:
        log.write(
            f"Validation report for the following boxes {target_box_numbers}\n ✅ Valid Files: {len(valid_files)}\n ❌ Corrupted Files: {len(corrupted_files)}\n"
        )
        for vf in valid_files:
            log.write(f"✅ Valid PDF: {vf}\n")
        for cf in corrupted_files:
            log.write(f"❌ Corrupted PDF: {cf}\n")

    json_report = {
        "metadata": {"base_path": base_path, "target_boxes": list(target_box_numbers)},
        "statistics": {
            "valid_files_count": len(valid_files),
            "corrupted_files_count": len(corrupted_files),
            "missing_boxes_count": len(missing_boxes) if missing_boxes else 0,
        },
        "output": {
            "valid_files": valid_files,
            "missing_boxes": list(missing_boxes) if missing_boxes else [],
            "corrupted_files": corrupted_files,
        },
    }

    json_file_path = log_file.replace(".log", ".json")
    with open(json_file_path, "w", encoding="utf-8") as jf:
        json.dump(json_report, jf, indent=4)

    print(f"\nDone! The text log of corrupted files was saved to: {log_file}")
    print(f"The structured JSON data was saved to: {json_file_path}")

    if upload_reports:
        remote_log_path = f"{base_path.rstrip('/')}/{os.path.basename(log_file)}"
        remote_json_path = f"{base_path.rstrip('/')}/{os.path.basename(json_file_path)}"

        print(f"Uploading reports back to the cloud ({base_path})...")
        try:
            provider.upload_file(log_file, remote_log_path)
            provider.upload_file(json_file_path, remote_json_path)
            print(
                f"✅ Upload successful! Files available at: {remote_log_path} and {remote_json_path}"
            )
        except Exception as e:
            print(f"❌ Failed to upload reports: {e}")


if __name__ == "__main__":
    s3_provider = S3Provider(bucket="digitization-dev")

    run_validation_pipeline(
        provider=s3_provider,  # cernbox_provider
        base_path="cern-archives/raw/PDF/",  # "teste/",
        log_file="s3_pdf_issues.log",
        data_source=sys.argv[1],  # public_link_hash
        upload_reports=int(sys.argv[2])
    )
