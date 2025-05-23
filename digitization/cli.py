import click
from .file_import.file_import import create_import_xml_files, get_matching_errors
from .xml_collect.xml_collect import records_collection
from .xml_collect.utils import (
    download_files_from_ftp,
    fix_white_spaces_in_directory,
    find_all_xmls,
    records_collection_creation,
)
import os
import logging

@click.group()
def digitization():
    pass

@digitization.command()
@click.option("--force", default=False, show_default=True, is_flag=True)
@click.option("--fix-eos-paths", default=False, show_default=True, is_flag=True)
@click.option("--fix-white-spaces", default=False, show_default=True, is_flag=True)
@click.option(
    "--create-collection-file", default=False, show_default=True, is_flag=True
)
def download(force, fix_eos_paths, fix_white_spaces, create_collection_file):
    """Download files from ftp."""

    click.echo("Downloading new files.")
    downloaded_directories = download_files_from_ftp(force=force)
    download_directory = os.getenv("DOWNLOAD_DIR", "/tmp/")

    if fix_white_spaces:
        click.echo("Fixing white spaces in directories and files.")
        for directory in downloaded_directories:
            fix_white_spaces_in_directory(os.path.join(download_directory, directory))

    if fix_eos_paths:
        click.echo("Fixing paths in xml.")
        find_all_xmls()

    if create_collection_file:
        click.echo("Creating collection file.")
        for directory in downloaded_directories:
            click.echo(f"File {directory}")
            records_collection_creation(
                os.path.join(download_directory, directory),
                os.path.join(download_directory, directory),
            )


@digitization.command("fix-eos-paths")
def fix_eos_paths():
    """Fix EOS paths."""
    click.echo("Fixing paths in xml")
    find_all_xmls()


@digitization.command("fix-white-spaces")
@click.option("-d", "--start-from-dir", type=str)
def fix_white_spaces(start_from_dir):
    """Fix white spaces."""
    click.echo(f"Fixing white spaces in directories and files. {start_from_dir}")
    fix_white_spaces_in_directory(start_from_dir)


@digitization.command("create-collection-file")
@click.option("-d", "--start-from-dir", type=str)
@click.option("-o", "--output-dir", type=str)
def create_collection_file(start_from_dir, output_dir):
    """Fix white spaces."""
    click.echo(
        f"Fixing white spaces in directories and files. {start_from_dir} and {output_dir}"
    )
    fix_white_spaces_in_directory(start_from_dir)
    records_collection(start_from_dir, output_dir)


@digitization.command("create-import-xml-files")
@click.option("-d", "--data-path", type=str, required=True, help="Path to the boite data files folder.")
@click.option("-o", "--output-path", type=str, required=True, help="Path to save the output logs and XML files.")
def create_import_xml(data_path, output_path):
    """Create XML files from the given data path. And logfile with missing files."""
    click.echo(f"Creating import XML files from {data_path} to {output_path}.")
    create_import_xml_files(data_path, output_path)
    click.echo("✅ XML files created successfully.")


@digitization.command("get-s3-matching-errors")
@click.option("-d", "--data-path", type=str, required=True, help="Path to the boite data files folder.")
@click.option("-o", "--log-path", type=str, required=True, help="Path to save the log file.")
def get_s3_matching_errors(data_path, log_path):
    """Log missing files in S3 and Excel."""
    log_file = os.path.join(log_path, "matching_errors.log")
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    for box_file in os.listdir(data_path):
        matching_errors = get_matching_errors(data_path, box_file, False)
        for filetype, missing_files in matching_errors["missing_in_excel"].items():
            if missing_files:
                logging.warning(
                    f"[{box_file}] Missing in Excel ({filetype}): {', '.join(missing_files)}"
                )
            else:
                logging.info(f"[{box_file}] No missing files in Excel for {filetype}.")
        for filetype, missing_files in matching_errors["missing_in_s3"].items():
            if missing_files:
                logging.warning(
                    f"[{box_file}] Missing in S3 ({filetype}): {', '.join(missing_files)}"
                )
            else:
                logging.info(f"[{box_file}] No missing files in S3 for {filetype}.")
    click.echo(f"✅ Log file created: {log_file}")


if __name__ == "__main__":
    digitization()
