import click
import ast
from .check_files.main import run_validation_pipeline
from refactory.storage_connection import S3Provider

from .file_import.boite_matcher import BoiteS3Matcher


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
    "-d",
    "--data-source",
    required=True,
    help="Boite Files. Supports a CERNBOX hash, range 1..10, or list [1,2].",
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
@click.option(
    "-p",
    "--base-path",
    default="cern-archives/raw/PDF/",
    show_default=True,
    help="Base S3 path to validate.",
)
def validate_files_integrity(data_source, base_path, bucket, upload_reports):
    """
    Validates files integrity and inventory alignment.
    This command checks for corrupted files and missing boxes.
    """

    inventory_input = parse_inventory(data_source)
    provider = S3Provider(bucket=bucket)

    try:
        run_validation_pipeline(
            provider=provider,
            base_path=base_path,
            log_file="s3_pdf_issues.log",
            data_source=inventory_input,
            upload_reports=upload_reports,
        )
        click.echo("Process finished. Check the generated logs for details.")
    except Exception as e:
        click.secho(f"Error: {e}", fg="red", err=True)


@digitization_v2.command("file-match")
@click.option(
    "-d",
    "--data-source",
    required=True,
    help="Target data source. Supports a local directory path or a CERNBOX URL.",
)
@click.option(
    "-p",
    "--base-path",
    default="cern-archives/raw/",
    show_default=True,
    help="Base S3 path to validate.",
)
@click.option(
    "-o",
    "--output-path",
    default="./match_results",
    show_default=True,
    help="Directory to save the generated JSON files (records and mismatches).",
)
@click.option(
    "-f",
    "--file-types",
    default="PDF,PDF_LATEX",
    show_default=True,
    help="Comma-separated list of file types to match (e.g., 'PDF,PDF_LATEX,TIFF').",
)
@click.option(
    "-b",
    "--bucket",
    default="digitization-dev",
    show_default=True,
    help="S3 Bucket name.",
)

def file_match(data_source, base_path, output_path, file_types, bucket):
    """
    Matches Boite Excel records against S3 files and generates JSON payloads.
    Generates a success JSON per Boite and a unified mismatch log.
    """

    CUSTOM_EXPIRATION = {
        # Example: uncomment the line below to test it
        # "PDF": 10,
        "PDF_LATEX": 45
    }

    provider = S3Provider(bucket=bucket, custom_expiration=CUSTOM_EXPIRATION)

    parsed_file_types = [t.strip() for t in file_types.split(",")]

    click.echo("Starting match process...")
    click.echo(f"Source: {data_source}")
    click.echo(f"File types: {', '.join(parsed_file_types)}")

    try:
        matcher = BoiteS3Matcher(
            provider=provider,
            base_path=base_path,
            data_source=data_source,
            output_path=output_path,
            file_types=parsed_file_types,
        )

        matcher.execute()

        click.secho(
            f"Match completed successfully. Output saved to: {output_path}", fg="green"
        )
    except Exception as e:
        click.secho(f"Error during matching: {e}", fg="red", err=True)


if __name__ == "__main__":
    digitization_v2()
