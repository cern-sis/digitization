import click
import ast
import os
from pathlib import Path

from .check_files.main import run_validation_pipeline
from refactory.storage_connection import S3Provider, CernboxProvider
from .file_import.boite_matcher import BoiteS3Matcher
from .file_import.xml_exporter import XMLExporter


def parse_inventory(value):
    """
    Parses the input to identify if it's a literal list,
    a range of IDs (1..10), or a single string/ID.
    """
    if isinstance(value, int) or value.isdigit():
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
            raise click.BadParameter(
                "Invalid range format. Use 'start..end' (e.g., 1..10)"
            )

    return value


@click.group()
def digitization_v2():
    pass


@digitization_v2.command("validate-files-integrity")
@click.option(
    "-d",
    "--data-source",
    required=True,
    help="Inventory source (CERNBOX hash, range 1..10, or list [1,2]).",
)
@click.option(
    "-u",
    "--upload-reports",
    is_flag=True,
    help="Upload validation reports back to storage.",
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
    help="Base S3 path.",
)
def validate_files_integrity(data_source, base_path, bucket, upload_reports):
    """Validates files integrity and inventory alignment."""
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
        click.echo("Process finished. Check logs for details.")
    except Exception as e:
        click.secho(f"Error: {e}", fg="red", err=True)


@digitization_v2.command("match-and-export")
@click.option(
    "-d", "--data-source", required=True, help="Local directory path or CERNBOX URL."
)
@click.option(
    "-p",
    "--base-paths",
    default="cern-archives/raw/",
    show_default=True,
    help="Comma-separated base S3 paths (e.g., cern-archives/raw/CORRECTIONS_2,cern-archives/raw/).",
)
@click.option(
    "-o",
    "--output-path",
    default="./results",
    show_default=True,
    help="Output directory.",
)
@click.option(
    "-f",
    "--file-types",
    default="PDF,PDF_LATEX",
    show_default=True,
    help="Comma-separated file types.",
)
@click.option(
    "-b",
    "--bucket",
    default="digitization-dev",
    show_default=True,
    help="S3 Bucket name.",
)
@click.option(
    "-x",
    "--generate-xml",
    is_flag=True,
    help="Generate XML files (FFT) for CDS upload.",
)
@click.option(
    "-c", "--upload-cernbox", is_flag=True, help="Upload XML files to CERNBox."
)
@click.option(
    "--cernbox-path",
    default="xml_exports",
    show_default=True,
    help="Target folder inside CERNBox.",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Stop script execution after matching. No XML or uploads will occur.",
)
@click.option(
    "-r",
    "--report",
    is_flag=True,
    help="Display detailed summary metrics and listed files in the console.",
)
def match_and_export(
    data_source,
    base_paths,
    output_path,
    file_types,
    bucket,
    generate_xml,
    upload_cernbox,
    cernbox_path,
    dry_run,
    report,
):
    """Matches Excel records against S3 and optionally exports to XML/CERNBox."""
    os.makedirs(output_path, exist_ok=True)

    provider = S3Provider(bucket=bucket)
    parsed_base_paths = [p.strip() for p in base_paths.split(",")]
    parsed_file_types = [t.strip() for t in file_types.split(",")]

    click.echo(f"Starting process for: {data_source}")

    try:
        matcher = BoiteS3Matcher(
            provider=provider,
            base_paths=parsed_base_paths,
            data_source=data_source,
            output_path=output_path,
            file_types=parsed_file_types,
        )

        results_map, all_mismatches = matcher.execute()

        total_records = sum(m["metrics"]["total_records"] for m in all_mismatches)
        total_matched = sum(m["metrics"]["total_matched"] for m in all_mismatches)
        total_unmatched = sum(m["metrics"]["total_unmatched"] for m in all_mismatches)

        if report:
            click.secho("\n=== RUN SUMMARY METRICS ===", fg="cyan", bold=True)
            click.echo(f"Total Records Processed : {total_records}")
            click.secho(f"Total Matched           : {total_matched}", fg="green")
            click.secho(f"Total Unmatched         : {total_unmatched}", fg="red")

            click.secho("\n--- Detailed File Matches ---", fg="cyan", bold=True)
            for box_data in all_mismatches:
                box_file = box_data["boite_file"]
                mets = box_data["metrics"]

                click.echo(
                    f"📦 {box_file}: {mets['total_matched']} matched | {mets['total_unmatched']} unmatched"
                )

                if box_data["mismatches"]["in_boite_missing_in_s3"]:
                    click.secho("   [Missing Records]", fg="yellow")
                    for missing in box_data["mismatches"]["in_boite_missing_in_s3"]:
                        missing_types = ", ".join(missing["missing_types"])
                        click.echo(
                            f"    - ID: {missing['record_id']} (Missing: {missing_types})"
                        )
            click.echo("===========================\n")
        else:
            click.secho(
                f"Match completed. [Matched: {total_matched} | Unmatched: {total_unmatched}]",
                fg="green",
            )

        if dry_run:
            click.secho(
                "⚠️ Dry-run mode active. Stopping execution before XML generation.",
                fg="yellow",
                bold=True,
            )
            return

        if generate_xml:
            if not results_map:
                click.secho("No valid records found to generate XML.", fg="yellow")
                return

            xml_output_folder = os.path.join(output_path, "xml_exports")
            os.makedirs(xml_output_folder, exist_ok=True)

            exporter = XMLExporter(output_path=xml_output_folder)
            report_data = exporter.generate_batch(results_map)

            click.secho(f"✅ XMLs generated in: {xml_output_folder}", fg="green")

            if upload_cernbox and report_data:
                _handle_cernbox_upload(report_data, cernbox_path)

    except Exception as e:
        click.secho(f"Critical Error: {e}", fg="red", err=True)


def _handle_cernbox_upload(report, remote_path):
    try:
        file_name = None
        cernbox = CernboxProvider()
        files = report.get("files", []).copy()

        if report.get("combined"):
            files.append(report["combined"])

        for local_file in files:
            file_name = Path(local_file).name
            target = f"{remote_path.strip('/')}/{file_name}"
            cernbox.upload_file(local_file_path=local_file, remote_file_path=target)
            click.echo(f"  -> Uploaded: {file_name}")

        click.secho("CERNBox sync complete.", fg="green")
    except Exception as e:
        click.secho(
            f"CERNBox Error: Failed to process '{file_name}'. Details: {e}",
            fg="red",
            err=True,
        )


if __name__ == "__main__":
    digitization_v2()
