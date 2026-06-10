import logging
import re
import tempfile
from common.storage_connection import StorageProvider, CernboxProvider
from check_files.utils import validate_pdf



logger = logging.getLogger(__name__)


def resolve_target_boxes(data_source: str | list[int]) -> set[int]:
    """Resolves the raw input into a definitive set of Box Numbers."""
    target_box_numbers = set()

    if (
        isinstance(data_source, str)
        and not data_source.replace(".", "").isdigit()
        and "[" not in data_source
    ):
        data_source_provider = CernboxProvider(public_link_hash=data_source)
        excel_files = data_source_provider.list_files("", ".xlsx")

        for file_path in excel_files:
            filename = file_path.split(".")[0]
            match = re.search(r"(?i:BOITE)[\-_]O0(\d+)(-\w+)?", filename)
            if match:
                target_box_numbers.add(int(match.group(1)))
    elif isinstance(data_source, list):
        target_box_numbers = set(data_source)

    return target_box_numbers


def validate_s3_files(
    provider: StorageProvider, base_path: str, target_box_numbers: set[int]
) -> dict:
    """Iterates through S3, validates PDFs, and returns a structured report dict."""
    logger.info(f"🔍 Starting S3 scan in path: {base_path}")

    folders = provider.list_folders(base_path)
    if not folders:
        logger.warning("No folders found in this path.")
        return {"error": "No folders found in this path."}

    found_and_valid_boxes = set()
    corrupted_files = []
    valid_files = []

    for folder in folders:
        match = re.search(r"(?i:BOITE)[\-_]O0(\d+)(-\w+)?", folder)
        if not match:
            continue

        box_num = int(match.group(1))
        if box_num not in target_box_numbers:
            continue

        pdf_files = provider.list_files(folder, "PDF")
        if not pdf_files:
            logger.warning(f"Box {box_num}: EMPTY FOLDER (No PDFs found)")
            continue

        found_and_valid_boxes.add(box_num)
        logger.info(f"Starting validation for Box {box_num} (Total: {len(pdf_files)} PDFs)")
        box_corrupted_count = 0

        for pdf_path in pdf_files:
            with tempfile.NamedTemporaryFile(delete=True) as tmp:
                provider.download_to_temp(pdf_path, tmp.name)

                if validate_pdf(tmp.name):
                    valid_files.append(pdf_path)
                else:
                    logger.warning(f"Corrupted file -> {pdf_path.split('/')[-1]}")
                    corrupted_files.append(pdf_path)
                    box_corrupted_count += 1

        if box_corrupted_count == 0:
            logger.info(f"Box {box_num} validated successfully. (0 corrupted files)")
        else:
            logger.warning(f"Box {box_num} validated with issues. ({box_corrupted_count} corrupted files)")

    missing_boxes = target_box_numbers - found_and_valid_boxes

    summary_text = (
        "\n========================================\n"
        "VALIDATION SUMMARY REPORT\n"
        "========================================\n"
        f"Valid PDFs       : {len(valid_files)}\n"
        f"Corrupted PDFs   : {len(corrupted_files)}\n"
        f"Missing Boxes    : {len(missing_boxes)}\n"
    )

    if corrupted_files:
        summary_text += "\nList of all corrupted files:\n"
        for cf in corrupted_files:
            summary_text += f"  - {cf}\n"
    summary_text += "========================================\n"

    logger.info(summary_text)

    return {
        "metadata": {"base_path": base_path, "target_boxes": list(target_box_numbers)},
        "statistics": {
            "valid_files_count": len(valid_files),
            "corrupted_files_count": len(corrupted_files),
            "missing_boxes_count": len(missing_boxes),
        },
        "output": {
            "valid_files": valid_files,
            "missing_boxes": list(missing_boxes),
            "corrupted_files": corrupted_files,
        },
    }
