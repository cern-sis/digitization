import boto3
import requests
import xml.etree.ElementTree as ET
from abc import ABC, abstractmethod
import os


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
        self,
        bucket: str,
        endpoint_url: str = "https://s3.cern.ch",
        custom_expiration: dict = None,
    ):
        self.bucket = bucket

        if os.environ["ACCESS_KEY"] and os.environ["SECRET_KEY"]:
                print("Logging into s3 using credentials provided in enviroment variables")
                self.s3 = boto3.client(
                    "s3",
                    aws_access_key_id=os.environ["ACCESS_KEY"],
                    aws_secret_access_key=os.environ["SECRET_KEY"],
                    endpoint_url=endpoint_url,
                )
        else:
                print("Using default s3 login without credentials")
                self.s3 = boto3.client(
                    "s3",
                    endpoint_url=endpoint_url,
                )
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
    def __init__(self, public_link_hash: str = None):
        self.account = os.getenv("CERNBOX_ACCOUNT")
        self.password = os.getenv("CERNBOX_PASSWORD")

        self.is_public = bool(public_link_hash)
        self.public_link_hash = public_link_hash

        if self.is_public:
            self.base_url = f"https://cernbox.cern.ch/remote.php/dav/public-files/{public_link_hash}"
            self.auth = None
        else:
            if not self.account or not self.password:
                raise ValueError(
                    "Error: CERN credentials required for protected shares.."
                )
            self.base_url = (
                f"https://api.cernbox.cern.ch/remote.php/dav/files/{self.account}"
            )
            self.auth = (self.account, self.password)

    def _propfind(self, path: str, depth: str = "1") -> list[str]:

        url = f"{self.base_url}/{path}/" if path else f"{self.base_url}/"
        headers = {"Depth": depth}

        response = requests.request("PROPFIND", url, headers=headers, auth=self.auth)
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
        url = f"{self.base_url}/{file_path}"
        response = requests.get(url, stream=True, auth=self.auth)
        response.raise_for_status()

        with open(temp_file_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

    def upload_file(self, local_file_path: str, remote_file_path: str) -> None:
        if self.is_public:
            raise NotImplementedError("Error: CERN credentials required for updates.")

        clean_remote_path = remote_file_path.strip("/")
        url = f"{self.base_url}/{clean_remote_path}"

        with open(local_file_path, "rb") as f:
            response = requests.put(url, data=f, auth=self.auth)
        response.raise_for_status()

    def generate_presigned_url(
        self, file_key: str, content_type: str = None, expiration: int = None
    ) -> str:
        return f"{self.base_url}/{file_key}"
