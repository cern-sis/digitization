import boto3
import tempfile
from PyPDF2 import PdfReader
import os

def get_s3_client():
    return boto3.client(
        's3',
        aws_access_key_id=os.environ['ACCESS_KEY'],
        aws_secret_access_key=os.environ['SECRET_KEY'],
        endpoint_url='https://s3.cern.ch',
    )


def is_pdf_valid(file_path: str) -> bool:
    """Check if a local PDF is valid."""
    try:
        with open(file_path, "rb") as f:
            header = f.read(8)
            f.seek(-20, 2)  # last 20 bytes
            trailer = f.read().strip()

        if not header.startswith(b"%PDF-"):
            return False
        if b"%%EOF" not in trailer:
            return False

        reader = PdfReader(file_path)
        _ = len(reader.pages)
        return True

    except Exception:
        return False


def check_s3_pdfs(bucket: str, base_prefix: str, start: int, end: int, log_file: str):
    """
    Iterate through BOITE_O0XXX folders under base_prefix, validate PDFs,
    and log corrupt files or empty subfolders.
    """
    s3 = get_s3_client()
    paginator = s3.get_paginator("list_objects_v2")

    with open(log_file, "w") as log:
        for num in range(start, end + 1):
            boite_prefix = f"{base_prefix}BOITE_O0{num:03d}/"
            print(f"\n📦 Checking {boite_prefix}")

            for result in paginator.paginate(Bucket=bucket, Prefix=boite_prefix, Delimiter="/"):
                for sub in result.get("CommonPrefixes", []):
                    subfolder = sub["Prefix"]

                    sub_files = s3.list_objects_v2(Bucket=bucket, Prefix=subfolder)
                    pdf_files = [
                        obj["Key"]
                        for obj in sub_files.get("Contents", [])
                        if obj["Key"].lower().endswith(".pdf")
                    ]

                    if not pdf_files:
                        msg = f"⚠️ Empty subfolder: {subfolder}\n"
                        print("   " + msg.strip())
                        log.write(msg)
                        continue

                    for key in pdf_files:
                        print(f"   📝 Checking {key} ... ", end="")
                        with tempfile.NamedTemporaryFile(delete=True) as tmp:
                            s3.download_file(bucket, key, tmp.name)
                            valid = is_pdf_valid(tmp.name)
                        if valid:
                            print("✅ Valid")
                        else:
                            print("❌ Corrupted")
                            log.write(f"Corrupted PDF: {key}\n")
                            
def check_pdf_latex_pdfs(bucket: str, base_prefix: str, start: int, end: int, log_file: str):
    """
    Check PDFs in PDF_LATEX/BOITE_O0XXX/ folders directly, and log corrupted files.
    """
    for num in range(start, end + 1):
        boite_prefix = f"{base_prefix}BOITE_O0{num:03d}/"
        print(f"\n📦 Checking PDFs in {boite_prefix}")

        s3 = get_s3_client()
        response = s3.list_objects_v2(Bucket=bucket, Prefix=boite_prefix)

        pdf_files = [
            obj["Key"]
            for obj in response.get("Contents", [])
            if obj["Key"].lower().endswith(".pdf")
        ]

        if not pdf_files:
            print("   ⚠️ No PDFs found.")
            continue

        for key in pdf_files:
            print(f"   📝 Checking {key} ... ", end="")
            with tempfile.NamedTemporaryFile(delete=True) as tmp:
                s3.download_file(bucket, key, tmp.name)
                valid = is_pdf_valid(tmp.name)
            if valid:
                print("✅ Valid")
            else:
                print("❌ Corrupted")
                with open(log_file, "a") as log:
                    log.write(f"❌ Corrupted PDF: {key}\n")



if __name__ == "__main__":
    check_pdf_latex_pdfs(
        bucket="cern-archives",
        base_prefix="raw/PDF/",
        start=454,
        end=718,
        log_file="s3_pdf_issues.log"
    )
