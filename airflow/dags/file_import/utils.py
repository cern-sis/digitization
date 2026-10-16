import os
import tempfile
import logging
from common.storage_connection import CernboxProvider
from common.utils import parse_cernbox_url

logger = logging.getLogger(__name__)


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
        raise RuntimeError(f"Failed to access CERNBox at {url}: {e}") from e

    if not xlsx_files:
        raise ValueError(f"No .xlsx files found at {url}")

    logger.info(
        f"Found {len(xlsx_files)} .xlsx files. Starting download to temp folder..."
    )

    failed_downloads = []
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
            failed_downloads.append(filename)

    if failed_downloads:
        raise RuntimeError(
            f"Failed to download {len(failed_downloads)} of {len(xlsx_files)} "
            f".xlsx files: {failed_downloads}"
        )

    return output_dir


def transform_box_file_name(box_file):
    return box_file.split(".")[0].upper().replace("-", "_")
