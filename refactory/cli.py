import click
import ast
from .main import run_validation_pipeline
from storage_connection import S3Provider


def parse_inventory(value):
    """
    Parses the input to identify if it's a literal list,
    a range of IDs (1..10), or a single string/ID.
    """
    if value.isdigit():
        return [int(value)]
    if value.startswith("[") and value.endswith("]"):
        try:
            return ast.literal_eval(value)
        except (ValueError, SyntaxError):
            raise click.BadParameter("Invalid list format. Use '[1, 2, 3]'")

    if ".." in value:
        try:
            start, end = map(int, value.split(".."))
            return list(range(start, end + 1))
        except ValueError:
            pass
    return value

@click.group()
def digitization_v2():
    pass


@digitization_v2.command("validate-files-integrity")
@click.option(
    "-s",
    "--inventory-source",
    required=True,
    help="Target inventory. Supports a CERNBOX hash, range 1..10, or list [1,2].",
)
@click.option(
    "-u",
    "--upload-reports",
    is_flag=True,
    help="Upload validation reports back to the storage provider.",
)
@click.option(
    "-b",
    "--bucket",
    default="digitization-dev",
    show_default=True,
    help="S3 Bucket name.",
)
def validate_files_integrity(inventory_source, upload_reports, bucket):
    """
    Validates files integrity and inventory alignment.
    This command checks for corrupted files and missing boxes.
    """

    inventory_input = parse_inventory(inventory_source)
    provider = S3Provider(bucket=bucket)

    try:
        run_validation_pipeline(
            provider=provider,
            base_path="cern-archives/raw/PDF/",
            log_file="s3_pdf_issues.log",
            inventory_source=inventory_input,
            upload_reports=upload_reports,
        )
        click.echo("Process finished. Check the generated logs for details.")
    except Exception as e:
        click.secho(f"Error: {e}", fg="red", err=True)


if __name__ == "__main__":
    digitization_v2()
