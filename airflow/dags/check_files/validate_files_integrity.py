import json
import tempfile
from datetime import datetime
from airflow.sdk import dag, task, Param

from common.storage_connection import S3Provider
from common.utils import parse_inventory
from check_files.validator import resolve_target_boxes, validate_s3_files


@dag(
    dag_id="validate_files_integrity",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["digitization", "validation"],
    params={
        "data_source": Param(
            "",
            type="string",
            description="Inventory source (e.g., 1..1000, [1,2], or CERNBOX hash)",
        ),
        "bucket": Param(
            "digitization-dev", type="string", description="Target S3 Bucket name"
        ),
        "base_path": Param(
            "cern-archives/raw/PDF/",
            type="string",
            description="Base S3 path for files",
        ),
        "upload_reports": Param(
            False,
            type="boolean",
            description="Toggle to upload validation reports back to storage",
        ),
    },
)
def validation_dag():

    @task(task_id="prepare_target_boxes")
    def prepare_targets_boxes(**context):
        """Parses inputs and figures out exactly which boxes to check."""
        raw_data_source = context["params"]["data_source"]

        inventory_input = parse_inventory(raw_data_source)

        targets = resolve_target_boxes(inventory_input)

        print(f"{len(targets)} boxes for validation.")
        return list(targets)

    @task(task_id="validate_s3_pdfs")
    def perform_validation(target_boxes: list, **context):
        """Iterates through S3 and checks if PDFs are corrupted."""
        bucket = context["params"]["bucket"]
        base_path = context["params"]["base_path"]

        provider = S3Provider(bucket=bucket)

        print(f"Starting validation for {len(target_boxes)} boxes in {base_path}")
        result_report = validate_s3_files(provider, base_path, set(target_boxes))

        return result_report

    @task(task_id="upload_validation_reports")
    def upload_reports(report: dict, **context):
        """Generates the JSON/TXT reports and uploads them securely to S3."""
        upload_toggle = context["params"]["upload_reports"]

        if not upload_toggle or "error" in report:
            print("Report upload skipped (Toggle is OFF or error occurred).")
            return

        bucket = context["params"]["bucket"]
        base_path = context["params"]["base_path"]
        provider = S3Provider(bucket=bucket)

        text_log = f"Validation report for boxes {report['metadata']['target_boxes']}\n"
        text_log += f"✅ Valid: {report['statistics']['valid_files_count']}\n"
        text_log += f"❌ Corrupted: {report['statistics']['corrupted_files_count']}\n\n"

        for vf in report["output"]["valid_files"]:
            text_log += f"✅ Valid PDF: {vf}\n"
        for cf in report["output"]["corrupted_files"]:
            text_log += f"❌ Corrupted PDF: {cf}\n"

        with (
            tempfile.NamedTemporaryFile(mode="w", delete=False) as json_tmp,
            tempfile.NamedTemporaryFile(mode="w", delete=False) as txt_tmp,
        ):
            json.dump(report, json_tmp, indent=4)
            txt_tmp.write(text_log)
            json_tmp_path, txt_tmp_path = json_tmp.name, txt_tmp.name

        remote_json_path = f"{base_path.rstrip('/')}/validation_report.json"
        remote_log_path = f"{base_path.rstrip('/')}/s3_pdf_issues.log"

        provider.upload_file(json_tmp_path, remote_json_path)
        provider.upload_file(txt_tmp_path, remote_log_path)

        print(f"Uploaded securely to {remote_json_path} and {remote_log_path}")

    targets = prepare_targets_boxes()
    validation_results = perform_validation(targets)
    upload_reports(validation_results)


validation_dag_instance = validation_dag()
