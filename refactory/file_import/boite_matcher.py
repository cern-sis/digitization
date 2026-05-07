import json
import os
import re
import pandas as pd
from pathlib import Path
from urllib.parse import urlparse

from .utils import fetch_boite_files, transform_box_file_name
from ..storage_connection import StorageProvider


class BoiteS3Matcher:
    def __init__(
        self,
        provider: StorageProvider,
        base_paths: list[str] | str,
        data_source: str,
        output_path: str,
        file_types: list[str] | None = None,
    ):
        self.provider = provider
        self.base_paths = base_paths if isinstance(base_paths, list) else [base_paths]
        self.output_path = Path(output_path) / "logs"
        self.output_path.mkdir(parents=True, exist_ok=True)
        self.file_types = file_types or [
            "PDF",
            "PDF_LATEX"
        ]
        self.data_path = self._prepare_data_path(data_source)

    def _is_url(self, value: str) -> bool:
        return urlparse(value).scheme in {"http", "https"}

    def _prepare_data_path(self, data_source: str) -> Path:
        if self._is_url(data_source):
            return Path(fetch_boite_files(data_source))
        return Path(data_source)

    def _get_base_filename(self, filename: str, ftype: str = "") -> str:
        lower_name = filename.lower()
        if lower_name.endswith("_latex.pdf"):
            return lower_name[:-10]
        if "." in lower_name:
            lower_name = lower_name.rsplit(".", 1)[0]

        if ftype == "TIFF":
            lower_name = re.sub(r"_\d{1,4}$", "", lower_name)

        return lower_name

    def _normalize_for_comparison(self, name: str) -> str:
        return re.sub(r"[^a-z0-9]", "", name.lower())

    def _load_s3_cache_for_boite(
        self, box_file: str
    ) -> tuple[dict[str, dict[str, list[str]]], dict[str, set[str]]]:
        cache: dict[str, dict[str, list[str]]] = {ft: {} for ft in self.file_types}
        available_keys: dict[str, set[str]] = {ft: set() for ft in self.file_types}
        mapped_roots: dict[str, dict[str, str]] = {ft: {} for ft in self.file_types}

        folder_pattern = re.compile(r"(?i:BOITE)[\-_]O0(\d+)(?:[\-_]\w+)?")
        match = folder_pattern.search(box_file)

        if not match:
            return cache, available_keys

        target_number = match.group(1)

        for filetype in self.file_types:
            for base_path in self.base_paths:
                prefix = f"{base_path}/{filetype}/BOITE_O0{target_number}".replace(
                    "\\", "/"
                ).replace("//", "/")
                all_raw_keys = self.provider.list_files(prefix)

                valid_keys: list[str] = []

                for key in all_raw_keys:
                    if key.endswith("/"):
                        continue

                    s3_match = folder_pattern.search(key)
                    if s3_match and s3_match.group(1) == target_number:
                        valid_keys.append(key)
                        available_keys[filetype].add(key)

                for key in valid_keys:
                    parts = key.split("/")
                    base_filename = self._get_base_filename(parts[-1], filetype)

                    if base_filename not in cache[filetype]:
                        cache[filetype][base_filename] = [key]
                        mapped_roots[filetype][base_filename] = base_path
                    elif mapped_roots[filetype][base_filename] == base_path:
                        if key not in cache[filetype][base_filename]:
                            cache[filetype][base_filename].append(key)

                for key in valid_keys:
                    parts = key.split("/")
                    if len(parts) > 1:
                        folder_name = self._get_base_filename(parts[-2], filetype)
                        if "boite" not in folder_name:
                            if folder_name not in cache[filetype]:
                                cache[filetype][folder_name] = [key]
                                mapped_roots[filetype][folder_name] = base_path
                            elif mapped_roots[filetype][folder_name] == base_path:
                                if key not in cache[filetype][folder_name]:
                                    cache[filetype][folder_name].append(key)

        return cache, available_keys

    def process_boite(self, box_file: str) -> tuple[list[dict], dict]:
        df = pd.read_excel(self.data_path / box_file, header=None)
        boite_name_s3 = transform_box_file_name(box_file)

        s3_cache, s3_available_keys = self._load_s3_cache_for_boite(box_file)

        records_data: list[dict] = []
        missing_in_s3: list[dict] = []
        used_s3_keys: dict[str, set[str]] = {ftype: set() for ftype in self.file_types}

        for _, row in df.iterrows():
            record_id, record_name = str(row[0]).strip(), str(row[1]).strip()
            search_name = self._get_base_filename(record_name)

            record_data: dict = {"record_id": record_id}
            missing_types: list[str] = []

            for ftype in self.file_types:
                matched_keys = s3_cache[ftype].get(search_name)

                if matched_keys:
                    matched_keys = sorted(matched_keys)

                    if ftype == "TIFF":
                        for i, m_key in enumerate(matched_keys, start=1):
                            url_key = f"{ftype.lower()}_{i:03d}_url"
                            record_data[url_key] = self.provider.generate_presigned_url(
                                m_key, ftype, None
                            )
                    else:
                        url_key = f"{ftype.lower()}_url"
                        content_type = (
                            "application/pdf" if ftype in ["PDF", "PDF_LATEX"] else None
                        )
                        record_data[url_key] = self.provider.generate_presigned_url(
                            matched_keys[0], ftype, content_type
                        )

                    used_s3_keys[ftype].update(matched_keys)
                else:
                    url_key = f"{ftype.lower()}_url"
                    record_data[url_key] = None
                    missing_types.append(ftype)

            if len(missing_types) == len(self.file_types):
                missing_in_s3.append(
                    {
                        "record_id": record_id,
                        "record_name": record_name,
                        "missing_types": missing_types,
                    }
                )
            records_data.append(record_data)

        missing_in_boite = [
            {"s3_key": key, "filetype": ftype}
            for ftype in self.file_types
            for key in (s3_available_keys[ftype] - used_s3_keys[ftype])
        ]

        near_matches = []
        for missing_rec in missing_in_s3:
            boite_norm = self._normalize_for_comparison(missing_rec["record_name"])

            for ftype in missing_rec["missing_types"]:
                unused_s3 = s3_available_keys[ftype] - used_s3_keys[ftype]

                for s3_key in unused_s3:
                    parts = s3_key.split("/")
                    s3_base = self._get_base_filename(parts[-1], ftype)
                    s3_norm = self._normalize_for_comparison(s3_base)

                    folder_norm = ""

                    if len(parts) > 1:
                        folder_norm = self._normalize_for_comparison(parts[-2])

                    if boite_norm == s3_norm or boite_norm == folder_norm:
                        near_matches.append(
                            {
                                "boite_record": missing_rec["record_name"],
                                "suggested_s3_key": s3_key,
                                "filetype": ftype,
                            }
                        )

        total_records = len(records_data)
        total_unmatched = len(missing_in_s3)
        total_matched = total_records - total_unmatched

        mismatch_data = {
            "boite_file": box_file,
            "s3_folder_name": boite_name_s3,
            "metrics": {
                "total_records": total_records,
                "total_matched": total_matched,
                "total_unmatched": total_unmatched,
            },
            "mismatches": {
                "in_boite_missing_in_s3": missing_in_s3,
                "in_s3_missing_in_boite": missing_in_boite,
                "potential_matches": near_matches,
            },
        }

        return records_data, mismatch_data

    def _export_records(self, box_file: str, records: list) -> None:
        base_name = box_file.rsplit(".", 1)[0]
        with open(
            self.output_path / f"{base_name}_records.json", "w", encoding="utf-8"
        ) as f:
            json.dump(records, f, indent=4, ensure_ascii=False)

    def _export_unified_log(self, all_mismatches: list) -> None:
        with open(
            self.output_path / "all_boites_mismatches.json", "w", encoding="utf-8"
        ) as f:
            json.dump(
                {"total": len(all_mismatches), "details": all_mismatches},
                f,
                indent=4,
                ensure_ascii=False,
            )

    def execute(self) -> dict[str, list[dict]]:
        results_map, all_mismatches = {}, []
        for box_file in os.listdir(self.data_path):
            if box_file.lower().endswith(".xlsx") and not box_file.startswith("~"):
                records, mismatches = self.process_boite(box_file)
                results_map[box_file] = records
                all_mismatches.append(mismatches)
                self._export_records(box_file, records)

        self._export_unified_log(all_mismatches)
        return results_map, all_mismatches
