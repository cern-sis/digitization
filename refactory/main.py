import tempfile
import re
import os
import sys
import json
from storage_connection import StorageProvider, S3Provider, CernboxProvider
from validate_pdf import is_pdf_valid

def run_validation_pipeline(provider: StorageProvider, base_path: str, log_file: str, start: int = None, end: int = None, upload_reports: bool = False):
    """Navigates directories, validates files, and logs corrupted files."""
    print(f"Discovering folders in: {base_path}")
    
    folders = provider.list_folders(base_path)
    
    if not folders:
        print("No folders found in this path.")
        return

    found_box_numbers = set()
    empty_folders = []
    corrupted_files = []
    valid_files_count = 0

    print("Starting validation...")

    for folder in folders:
        if "BOITE_" not in folder:
            continue

        match = re.search(r"BOITE_O0(\d+)", folder)
        if match:
            box_num = int(match.group(1))
            if start is not None and box_num < start:
                continue
            if end is not None and box_num > end:
                continue
            
            found_box_numbers.add(box_num)
        else:
            continue
            
        print(f"\nChecking folder: {folder}")
        pdf_files = provider.list_pdfs(folder)

        if not pdf_files:
            print("   Empty folder or no PDFs.")
            empty_folders.append(folder)
            continue

        for file_path in pdf_files:
            print(f"   {file_path.split('/')[-1]} ... ", end="")
            
            with tempfile.NamedTemporaryFile(delete=True) as tmp:
                provider.download_to_temp(file_path, tmp.name)
                valid = is_pdf_valid(tmp.name)
            
            if valid:
                print("✅ Valid")
                valid_files_count += 1
            else:
                print("❌ Corrupted")
                corrupted_files.append(file_path)

    with open(log_file, "w", encoding="utf-8") as log:
        for cf in corrupted_files:
            log.write(f"Corrupted PDF: {cf}\n")

    missing_boxes = []
    if start is not None and end is not None:
        expected_boxes = set(range(start, end + 1))
        missing_boxes = sorted(list(expected_boxes - found_box_numbers))

    json_report = {
        "metadata": {
            "base_path": base_path,
            "range_analyzed": {"start": start, "end": end}
        },
        "statistics": {
            "valid_files": valid_files_count,
            "corrupted_files_count": len(corrupted_files),
            "empty_folders_count": len(empty_folders),
            "missing_boxes_count": len(missing_boxes) if missing_boxes else 0
        },
        "issues": {
            "missing_boxes": missing_boxes if missing_boxes else [],
            "empty_folders": empty_folders,
            "corrupted_files": corrupted_files
        }
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
            print(f"✅ Upload successful! Files available at: {remote_log_path} and {remote_json_path}")
        except Exception as e:
            print(f"❌ Failed to upload reports: {e}")

if __name__ == "__main__":
    s3_provider = S3Provider(bucket="digitization-dev")
    # cernbox_provider = CernboxProvider(public_link_hash="XjjFxUWUMpuTYCz")
    
    run_validation_pipeline(
        provider=s3_provider, #cernbox_provider
        base_path="cern-archives/raw/PDF/", #"teste/", 
        log_file="s3_pdf_issues.log",
        start=int(sys.argv[1]), #123
        end=int(sys.argv[2]), #126
        upload_reports=sys.argv[3] # 0 | 1 
    )