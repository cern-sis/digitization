import json
import tempfile
import logging
from pathlib import Path
from datetime import datetime
from airflow.sdk import dag, task, Param

from common.storage_connection import S3Provider, CernboxProvider
from file_import.boite_matcher import BoiteS3Matcher
from file_import.xml_exporter import XMLExporter

logger = logging.getLogger(__name__)


@dag(
    dag_id="match_and_export",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["digitization", "export", "xml"],
    params={
        "data_source": Param(
            "", type="string", description="Local directory path or CERNBOX URL"
        ),
        "base_paths": Param(
            "cern-archives/raw/",
            type="string",
            description="Comma-separated base S3 paths (e.g., path/1, path/2)",
        ),
        "file_types": Param(
            "PDF,PDF_LATEX", type="string", description="Comma-separated file types"
        ),
        "bucket": Param(
            "digitization-dev", type="string", description="Target S3 Bucket name"
        ),
        "execution_mode": Param(
            "Full Export (CERNBox)",
            type="string",
            enum=[
                "Dry Run",
                "Generate XMLs (Temp Folder Only)",
                "Full Export (CERNBox)",
            ],
            description="Select the behavior of the pipeline.",
        ),
        "cernbox_path": Param(
            "xml_exports",
            type="string",
            description="Target folder path inside CERNBox",
        ),
    },
)
def match_and_export_dag():

    @task(task_id="match_s3_records")
    def match_records(**context) -> dict:
        params = context["params"]
        bucket = params["bucket"]
        provider = S3Provider(bucket=bucket)

        parsed_base_paths = [p.strip() for p in params["base_paths"].split(",")]
        parsed_file_types = [t.strip() for t in params["file_types"].split(",")]

        logger.info(f"Starting match process for data_source: {params['data_source']}")

        matcher = BoiteS3Matcher(
            provider=provider,
            base_paths=parsed_base_paths,
            data_source=params["data_source"],
            file_types=parsed_file_types,
        )

        results_map, all_mismatches = matcher.execute()

        total_records = sum(m["metrics"]["total_records"] for m in all_mismatches)
        total_matched = sum(m["metrics"]["total_matched"] for m in all_mismatches)
        total_unmatched = sum(m["metrics"]["total_unmatched"] for m in all_mismatches)

        summary = (
            "\n===========================\n"
            "=== RUN SUMMARY METRICS ===\n"
            f"Total Records Processed : {total_records}\n"
            f"Total Matched           : {total_matched}\n"
            f"Total Unmatched         : {total_unmatched}\n"
            "===========================\n"
        )
        logger.info(summary)

        logger.info("Uploading JSON mismatch logs back to S3...")
        with tempfile.NamedTemporaryFile(
            mode="w", delete=False, encoding="utf-8"
        ) as tmp_log:
            json.dump(
                {"total": len(all_mismatches), "details": all_mismatches},
                tmp_log,
                indent=4,
            )
            tmp_log_path = tmp_log.name

        log_remote_path = f"{parsed_base_paths[0].rstrip('/')}/logs/all_boites_mismatches_{context['run_id']}.json"
        provider.upload_file(tmp_log_path, log_remote_path)
        logger.info(f"Mismatch logs saved to S3: {log_remote_path}")

        return results_map

    @task(task_id="export_and_upload_xmls")
    def generate_and_upload_xmls(results_map: dict, **context):
        """Generates XMLs and dynamically routes them based on the selected execution mode."""
        params = context["params"]
        mode = params["execution_mode"]

        if mode == "Dry Run":
            logger.info(
                "Dry-run mode active. Stopping execution before XML generation."
            )
            return

        if not results_map:
            logger.info("No valid records found to generate XML. Skipping.")
            return

        logger.info("Generating XML files in temporary memory...")
        exporter = XMLExporter()
        report_data = exporter.generate_batch(results_map)

        if mode == "Full Export (CERNBox)" and report_data.get("files"):
            logger.info(f"Starting CERNBox upload to path: {params['cernbox_path']}")
            cernbox = CernboxProvider()

            files_to_upload = report_data.get("files", []).copy()
            if report_data.get("combined"):
                files_to_upload.append(report_data["combined"])

            for local_file in files_to_upload:
                file_name = Path(local_file).name
                target = f"{params['cernbox_path'].strip('/')}/{file_name}"
                try:
                    cernbox.upload_file(
                        local_file_path=local_file, remote_file_path=target
                    )
                    logger.info(f"   -> Uploaded successfully: {file_name}")
                except Exception as e:
                    logger.error(f"   CERNBox Upload Failed for {file_name}: {e}")
                    raise e

            logger.info("CERNBox sync complete.")
        else:
            logger.info(
                "Execution Mode set to Temp Folder Only. Files generated successfully but NOT uploaded."
            )


    matched_data = match_records()
    generate_and_upload_xmls(matched_data)



match_and_export_dag_instance = match_and_export_dag()
