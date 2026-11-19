import xml.etree.ElementTree as ET
from abc import ABC, abstractmethod

import requests
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.http.hooks.http import HttpHook


class StorageProvider(ABC):
    """Base interface for storage providers."""

    @abstractmethod
    def list_folders(self, base_path: str) -> list[str]:
        pass

    @abstractmethod
    def list_files(self, folder_path: str, extension: str = None) -> list[str]:
        pass

    @abstractmethod
    def download_to_temp(self, file_path: str, temp_file_path: str) -> None:
        pass

    @abstractmethod
    def upload_file(self, local_file_path: str, remote_file_path: str) -> None:
        pass

    @abstractmethod
    def generate_presigned_url(
        self, file_key: str, content_type: str = None, expiration: int = 31556952
    ) -> str:
        pass


class S3Provider(StorageProvider):
    def __init__(
        self, bucket: str, custom_expiration: dict = None, conn_id: str = "aws_s3_cern"
    ):
        self.bucket = bucket

        hook = S3Hook(aws_conn_id=conn_id)

        self.s3 = hook.get_conn()

        self.expiration_config = {
            "PDF": 365,
            "PDF_LATEX": 90,
            "TIFF": 30,
            "DEFAULT": 365,
        }

        if custom_expiration:
            self.expiration_config.update(custom_expiration)

    def list_folders(self, base_path: str) -> list[str]:
        paginator = self.s3.get_paginator("list_objects_v2")
        folders = []
        for page in paginator.paginate(
            Bucket=self.bucket, Prefix=base_path, Delimiter="/"
        ):
            for prefix in page.get("CommonPrefixes", []):
                folders.append(prefix["Prefix"])
        return folders

    def list_files(self, folder_path: str, extension: str = None) -> list[str]:
        paginator = self.s3.get_paginator("list_objects_v2")
        files = []
        for page in paginator.paginate(Bucket=self.bucket, Prefix=folder_path):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if not key.endswith("/"):
                    if extension is None or key.lower().endswith(extension.lower()):
                        files.append(key)
        return files

    def download_to_temp(self, file_path: str, temp_file_path: str) -> None:
        self.s3.download_file(self.bucket, file_path, temp_file_path)

    def upload_file(self, local_file_path: str, remote_file_path: str) -> None:
        self.s3.upload_file(local_file_path, self.bucket, remote_file_path)

    def generate_presigned_url(
        self, file_key: str, file_type: str, content_type: str = None
    ) -> str:
        expiration = 86400 * (
            self.expiration_config.get(file_type, self.expiration_config["DEFAULT"])
        )

        params = {"Bucket": self.bucket, "Key": file_key}
        if content_type:
            params["ResponseContentType"] = content_type

        return self.s3.generate_presigned_url(
            ClientMethod="get_object",
            Params=params,
            ExpiresIn=expiration,
        )


class CernboxProvider(StorageProvider):
    def __init__(self, public_link_hash: str = None, conn_id: str = "cernbox_conn"):
        self.is_public = bool(public_link_hash)
        self.public_link_hash = public_link_hash
        self.account = None

        if self.is_public:
            self.base_url = f"https://cernbox.cern.ch/remote.php/dav/public-files/{public_link_hash}"
            self.session = requests.Session()
        else:
            self.hook = HttpHook(http_conn_id=conn_id)
            conn = self.hook.get_connection(conn_id)
            host = conn.host or "https://api.cernbox.cern.ch"

            self.account = conn.login

            self.base_url = f"{host}/remote.php/dav/files/{self.account}"
            self.session = self.hook.get_conn()

    def _build_eos_path(self, path: str) -> str:
        clean_path = path.lstrip("/")

        if clean_path.startswith("eos/"):
            return clean_path

        if self.account and not self.is_public:
            initial = self.account[0].lower()
            return f"eos/user/{initial}/{self.account}/{clean_path}"

        return clean_path

    def _propfind(self, path: str, depth: str = "1") -> list[str]:
        eos_path = self._build_eos_path(path)
        url = f"{self.base_url}/{eos_path}/" if eos_path else f"{self.base_url}/"

        headers = {"Depth": depth}

        response = self.session.request("PROPFIND", url, headers=headers)
        response.raise_for_status()

        root = ET.fromstring(response.content)
        namespaces = {"d": "DAV:"}

        paths = []
        for response_tag in root.findall("d:response", namespaces)[1:]:
            href = response_tag.find("d:href", namespaces).text
            filename = href.rstrip("/").split("/")[-1]
            paths.append(filename)

        return paths

    def list_folders(self, base_path: str) -> list[str]:
        raise NotImplementedError("This method is not available for this storage type.")

    def list_files(self, folder_path: str, extension: str = None) -> list[str]:
        all_items = self._propfind(folder_path)
        if extension:
            return [p for p in all_items if p.lower().endswith(extension.lower())]
        return all_items

    def download_to_temp(self, file_path: str, temp_file_path: str) -> None:
        eos_path = self._build_eos_path(file_path)
        url = f"{self.base_url}/{eos_path}"

        response = self.session.get(url, stream=True)
        response.raise_for_status()

        with open(temp_file_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

    def create_folder(self, folder_path: str) -> None:
        if self.is_public or not self.account:
            raise ValueError("Error: CERN credentials required to create folders.")

        eos_path = self._build_eos_path(folder_path)
        url = f"{self.base_url}/{eos_path}/"

        response = self.session.request("MKCOL", url)

        if response.status_code not in (201, 405):
            response.raise_for_status()

    def upload_file(self, local_file_path: str, remote_file_path: str) -> None:
        if self.is_public or not self.account:
            raise ValueError("Error: CERN account is required for uploading.")

        eos_path = self._build_eos_path(remote_file_path)
        url = f"{self.base_url}/{eos_path}"

        with open(local_file_path, "rb") as f:
            response = self.session.put(url, data=f)

        if response.status_code == 409:
            clean_remote_path = remote_file_path.strip("/")
            parent_dir = "/".join(clean_remote_path.split("/")[:-1])

            if parent_dir:
                self.create_folder(parent_dir)

                with open(local_file_path, "rb") as retry_f:
                    retry_response = self.session.put(url, data=retry_f)

                retry_response.raise_for_status()
                return

        response.raise_for_status()

    def generate_presigned_url(
        self, file_key: str, content_type: str = None, expiration: int = None
    ) -> str:
        eos_path = self._build_eos_path(file_key)
        return f"{self.base_url}/{eos_path}"
