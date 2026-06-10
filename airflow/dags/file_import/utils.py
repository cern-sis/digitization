import os
import tempfile
import logging
from urllib.parse import urlparse
from common.storage_connection import CernboxProvider

logger = logging.getLogger(__name__)


def parse_cernbox_url(url: str) -> dict:
    parsed = urlparse(url)
    path = parsed.path

    if "/s/" in path:
        hash_code = path.split("/s/")[-1].split("/")[0]
        return {"public_link_hash": hash_code, "eos_path": None}
    elif "public-files" in path:
        hash_code = path.split("public-files/")[-1].split("/")[0]
        return {"public_link_hash": hash_code, "eos_path": None}
    elif "eos" in path:
        eos_path = "eos" + path.split("eos")[-1]
        return {"public_link_hash": None, "eos_path": eos_path}
    else:
        raise ValueError(f"Invalid CERNBox URL format: {url}")


def fetch_boite_files(url: str, output_dir: str = None) -> str:
    """Downloads all .xlsx files from a CERNBox URL."""
    logger.info(f"Fetching Excel files from URL: {url}")

    parsed_data = parse_cernbox_url(url)
    provider = CernboxProvider(public_link_hash=parsed_data["public_link_hash"])

    if output_dir is None:
        output_dir = tempfile.mkdtemp(prefix="boite_data_")
    elif not os.path.exists(output_dir):
        os.makedirs(output_dir)

    target_path = parsed_data["eos_path"] if parsed_data["eos_path"] else ""

    try:
        xlsx_files = provider.list_files(target_path, extension=".xlsx")
    except Exception as e:
        logger.error(f"Failed to access CERNBox. Error: {e}")
        return output_dir

    if not xlsx_files:
        logger.warning("No .xlsx files found in the target directory.")
        return output_dir

    logger.info(
        f"Found {len(xlsx_files)} .xlsx files. Starting download to temp folder..."
    )

    for filename in xlsx_files:
        local_path = os.path.join(output_dir, filename)
        remote_file_path = (
            f"{target_path.rstrip('/')}/{filename}" if target_path else filename
        )

        try:
            logger.info(f"   -> Downloading: {filename}")
            provider.download_to_temp(remote_file_path, local_path)
        except Exception as e:
            logger.error(f"   ❌ Failed to download {filename}: {e}")

    return output_dir


def transform_box_file_name(box_file):
    return box_file.split(".")[0].upper().replace("-", "_")
