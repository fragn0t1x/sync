import os
import requests


class YandexDiskStorage:
    BASE_URL = "https://cloud-api.yandex.net/v1/disk/resources"

    def __init__(self, token: str, cloud_folder: str):
        self.headers = {"Authorization": f"OAuth {token}"}
        self.cloud_folder = cloud_folder
        self._ensure_folder_exists()

    def _ensure_folder_exists(self):
        """Создает папку в облаке, если она не существует"""
        params = {"path": self.cloud_folder}
        response = requests.put(self.BASE_URL, headers=self.headers, params=params)

        if response.status_code == 409:  # Папка уже существует
            return
        if response.status_code not in [201, 200]:
            raise ConnectionError(
                f"Ошибка создания папки: {response.json().get('message')}"
            )

    def _get_upload_link(self, filename: str, overwrite: bool = False):
        """Получает ссылку для загрузки файла"""
        path = f"{self.cloud_folder}/{filename}"
        params = {"path": path, "overwrite": str(overwrite).lower()}
        response = requests.get(
            f"{self.BASE_URL}/upload", headers=self.headers, params=params
        )

        if response.status_code != 200:
            raise ConnectionError(
                f"Ошибка получения ссылки: {response.json().get('message')}"
            )
        return response.json()["href"]

    def load(self, local_path: str):
        """Загрузка нового файла"""
        filename = os.path.basename(local_path)
        upload_url = self._get_upload_link(filename, overwrite=False)

        with open(local_path, "rb") as file:
            response = requests.put(upload_url, files={"file": file})
        if response.status_code != 201:
            raise ConnectionError(f"Ошибка загрузки: {response.status_code}")

    def reload(self, local_path: str):
        """Обновление существующего файла"""
        filename = os.path.basename(local_path)
        upload_url = self._get_upload_link(filename, overwrite=True)

        with open(local_path, "rb") as file:
            response = requests.put(upload_url, files={"file": file})
        if response.status_code != 201:
            raise ConnectionError(f"Ошибка обновления: {response.status_code}")

    def delete(self, filename: str):
        """Удаление файла"""
        path = f"{self.cloud_folder}/{filename}"
        params = {"path": path}
        response = requests.delete(self.BASE_URL, headers=self.headers, params=params)

        if response.status_code not in [200, 202, 204]:
            raise ConnectionError(f"Ошибка удаления: {response.json().get('message')}")

    def get_info(self) -> set:
        """Получение списка файлов в облаке"""
        params = {"path": self.cloud_folder}
        response = requests.get(self.BASE_URL, headers=self.headers, params=params)

        if response.status_code != 200:
            raise ConnectionError(
                f"Ошибка получения информации: {response.json().get('message')}"
            )

        files = response.json().get("_embedded", {}).get("items", [])
        return {item["name"] for item in files if item["type"] == "file"}
