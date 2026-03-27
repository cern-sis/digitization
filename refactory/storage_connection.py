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
    def list_pdfs(self, folder_path: str) -> list[str]:
        pass

    @abstractmethod
    def download_to_temp(self, file_path: str, temp_file_path: str) -> None:
        pass
        
    @abstractmethod
    def upload_file(self, local_file_path: str, remote_file_path: str) -> None:
        pass

class S3Provider(StorageProvider):
    def __init__(self, bucket: str, endpoint_url: str = 'https://s3.cern.ch'):
        self.bucket = bucket
        self.s3 = boto3.client('s3', endpoint_url=endpoint_url)

    def list_folders(self, base_path: str) -> list[str]:
        paginator = self.s3.get_paginator('list_objects_v2')
        folders = []
        for page in paginator.paginate(Bucket=self.bucket, Prefix=base_path, Delimiter='/'):
            for prefix in page.get('CommonPrefixes', []):
                folders.append(prefix['Prefix'])
        return folders

    def list_pdfs(self, folder_path: str) -> list[str]:
        response = self.s3.list_objects_v2(Bucket=self.bucket, Prefix=folder_path)
        return [
            obj['Key'] for obj in response.get('Contents', [])
            if obj['Key'].lower().endswith('.pdf')
        ]

    def download_to_temp(self, file_path: str, temp_file_path: str) -> None:
        self.s3.download_file(self.bucket, file_path, temp_file_path)

    def upload_file(self, local_file_path: str, remote_file_path: str) -> None:
        self.s3.upload_file(local_file_path, self.bucket, remote_file_path)

class CernboxProvider(StorageProvider):
    def __init__(self, public_link_hash: str, account: str = None, password: str = None):
        self.public_link_hash = public_link_hash
        self.account = account
        self.password = password
        
        self.public_base_url = f"https://cernbox.cern.ch/remote.php/dav/public-files/{public_link_hash}"

        if account:
            self.auth_base_url = f"https://cernbox.cern.ch/remote.php/dav/files/{account}"

    def _propfind(self, path: str, depth: str = "1") -> list[str]:
        url = f"{self.public_base_url}/{path}".rstrip('/') + '/'
        headers = {'Depth': depth}
        
        response = requests.request('PROPFIND', url, headers=headers)
        response.raise_for_status()

        root = ET.fromstring(response.content)
        namespaces = {'d': 'DAV:'}
        
        paths = []
        for response_tag in root.findall('d:response', namespaces)[1:]:
            href = response_tag.find('d:href', namespaces).text
            relative_path = href.split(f"/public-files/{self.public_link_hash}/")[-1]
            paths.append(relative_path)
        return paths

    def list_folders(self, base_path: str) -> list[str]:
        all_items = self._propfind(base_path)
        return [p for p in all_items if p.endswith('/') or "BOITE_" in p]

    def list_pdfs(self, folder_path: str) -> list[str]:
        all_items = self._propfind(folder_path)
        return [p for p in all_items if p.lower().endswith('.pdf')]

    def download_to_temp(self, file_path: str, temp_file_path: str) -> None:
        url = f"{self.public_base_url}/{file_path}"
        response = requests.get(url, stream=True)
        response.raise_for_status()
        with open(temp_file_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

    def upload_file(self, local_file_path: str, remote_file_path: str) -> None:
        if not self.account or not self.password:
            raise ValueError("CERN account and password are required for uploading.")
            
        clean_remote_path = remote_file_path.lstrip('/')
        url = f"{self.auth_base_url}/{clean_remote_path}"
        
        with open(local_file_path, 'rb') as f:
            response = requests.put(url, data=f, auth=(self.account, self.password))
            
        response.raise_for_status()