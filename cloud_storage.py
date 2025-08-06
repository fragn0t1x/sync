import requests
from pathlib import Path


class YandexDiskStorage:
    def __init__(self, token: str, cloud_folder: str):
        self.token = token
        self.cloud_folder = cloud_folder
        self.base_url = "https://cloud-api.yandex.net/v1/disk/resources"
        self.headers = {"Authorization": f"OAuth {self.token}"}

    def _get_upload_url(self, cloud_path: str, overwrite: bool = False) -> str:
        url = f"{self.base_url}/upload"
        params = {"path": cloud_path, "overwrite": "true" if overwrite else "false"}
        response = requests.get(url, headers=self.headers, params=params)
        response.raise_for_status()
        return response.json()["href"]

    def load(self, path: Path) -> None:
        cloud_path = f"{self.cloud_folder}/{path.name}"
        upload_url = self._get_upload_url(cloud_path, overwrite=False)
        with path.open("rb") as f:
            response = requests.put(upload_url, data=f)
            response.raise_for_status()

    def reload(self, path: Path) -> None:
        cloud_path = f"{self.cloud_folder}/{path.name}"
        upload_url = self._get_upload_url(cloud_path, overwrite=True)
        with path.open("rb") as f:
            resp = requests.put(upload_url, data=f)
            resp.raise_for_status()

    def delete(self, filename: str) -> None:
        url = self.base_url
        cloud_path = f"{self.cloud_folder}/{filename}"
        params = {"path": cloud_path}
        response = requests.delete(url, headers=self.headers, params=params)
        response.raise_for_status()

    def get_info(self) -> list[str]:
        url = self.base_url
        params = {"path": self.cloud_folder, "limit": 1000}
        response = requests.get(url, headers=self.headers, params=params)
        response.raise_for_status()
        data = response.json()
        items = data.get("_embedded", {}).get("items", [])
        return [item["name"] for item in items if item["type"] == "file"]
