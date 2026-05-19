import re
import xml.etree.ElementTree as ET
from xml.dom import minidom
import tempfile
from pathlib import Path


class XMLExporter:
    """Handles XML generation and file persistence logic."""

    def __init__(self, output_path: str | None = None):

        if output_path:
            self.base_dir = Path(output_path)
            self.base_dir.mkdir(parents=True, exist_ok=True)
        else:
            self.base_dir = Path(tempfile.mkdtemp(prefix="boite_xmls_"))

    def _get_description_for_type(self, url_key: str) -> str:
        base_type = url_key.replace("_url", "")
        clean_type = re.sub(r"_\d{3,4}$", "", base_type)

        type_mapping = {
            "pdf": "Fulltext PDF",
            "pdf_latex": "Fulltext PDF_LaTeX",
            "pdf_ocr": "Fulltext PDF_OCR",
            "pdf_transmis": "Fulltext PDF_TRANSMIS",
            "tiff": "Fulltext TIFF",
        }

        return type_mapping.get(clean_type, f"Fulltext {clean_type.upper()}")

    def _build_record_element(self, root: ET.Element, record: dict) -> None:
        record_node = ET.SubElement(root, "record")
        ET.SubElement(record_node, "controlfield", tag="001").text = str(
            record.get("record_id", "")
        )

        for key, value in record.items():
                if key.endswith("_url") and value:
                    description = self._get_description_for_type(key)

                    df = ET.SubElement(record_node, "datafield", tag="FFT", ind1=" ", ind2=" ")
                    ET.SubElement(df, "subfield", code="a").text =  value
                    ET.SubElement(df, "subfield", code="t").text = "Main"
                    ET.SubElement(df, "subfield", code="d").text = description

    def _save_to_disk(self, root: ET.Element, filename: str) -> str:
        """Converts element tree to XML file."""
        rough_string = ET.tostring(root, encoding="utf-8")
        pretty_xml = minidom.parseString(rough_string).toprettyxml(indent="  ")


        file_path = self.base_dir / filename
        file_path.write_text(pretty_xml, encoding="utf-8")
        return str(file_path)

    def generate_single(self, records: list[dict], filename: str) -> str | None:
        """Generates XML file for Boite file."""
        root = ET.Element("collection")
        valid_records_count = 0

        for rec in records:
            has_valid_url = any(key.endswith("_url") and val for key, val in rec.items())

            if not has_valid_url:
                continue

            self._build_record_element(root, rec)
            valid_records_count += 1

        if valid_records_count == 0:
            print(f" {filename} Skipped: No valid files found.")
            return None

        return self._save_to_disk(root, filename)

    def generate_batch(self, results_map: dict[str, list[dict]]) -> dict:
        """Batch generates individual XMLs and a combined output from boite files."""
        output_report={
            "output_path": str(self.base_dir),
            "files":[],
            "combined":None
        }

        all_records_combined = []

        for boite_file, records in results_map.items():
            if not records:
                continue

            xml_name = str(Path(boite_file).with_suffix(".xml"))
            saved_file_path = self.generate_single(records, xml_name)

            if saved_file_path:
                output_report["files"].append(saved_file_path)
                all_records_combined.extend(records)

        if all_records_combined:
            output_report["combined"] = self.generate_single(
                all_records_combined, "Boites_combined.xml"
            )

        return output_report
