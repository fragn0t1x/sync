import os
import time
import logging
from pathlib import Path
from configparser import ConfigParser

# Добавляем сюда облачные сервисы
from cloud_storage import YandexDiskStorage


def setup_logger(log_path: str) -> logging.Logger:
    logger = logging.getLogger("sync")
    logger.setLevel(logging.INFO)
    handler = logging.FileHandler(log_path, encoding="utf-8")
    formatter = logging.Formatter("%(asctime)s %(levelname)s:%(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


def read_config(config_path: str | Path) -> dict[str, str]:
    config = ConfigParser()
    config.read(config_path)
    params = config["Settings"]
    return {
        "local_folder": params.get("local_folder", ""),
        "cloud_folder_name": params.get("cloud_folder_name", ""),
        "token": params.get("token", ""),
        "sync_interval": params.get("sync_interval", "60"),
        "log_file": params.get("log_file", "sync.log"),
    }


def validate_settings(settings: dict[str, str]) -> None:
    folder = Path(settings["local_folder"])
    if not folder.exists() or not folder.is_dir():
        raise ValueError("Локальная папка не существует или путь указан неверно.")
    if not settings["token"]:
        raise ValueError("Токен не задан или задан неверно.")


def sync(
    local_folder: Path, storage: YandexDiskStorage, logger: logging.Logger
) -> None:
    try:
        cloud_files = set(storage.get_info())
        local_files = {f.name for f in local_folder.iterdir() if f.is_file()}

        # Добавление новых и изменённых файлов

        # Добавь в данное множество названия файлов, которые не нужно загружать через запятую
        EXCLUDE_FILES = {".DS_Store"}
        for file in local_folder.iterdir():
            if not file.is_file():
                continue
            if file.name in EXCLUDE_FILES:
                continue  # пропускаем файл
            if file.is_file():
                try:
                    if file.name not in cloud_files:
                        storage.load(file)
                        logger.info(f"Загружен файл: {file.name}")
                    else:
                        storage.reload(file)
                        logger.info(f"Обновлён файл: {file.name}")
                except Exception as e:
                    logger.error(f"Ошибка при загрузке файла {file.name}: {e}")

        # Удаление файлов
        for cloud_file in cloud_files - local_files:
            try:
                storage.delete(cloud_file)
                logger.info(f"Удалён файл: {cloud_file}")
            except Exception as e:
                logger.error(f"Ошибка при удалении файла {cloud_file}: {e}")
    except Exception as e:
        logger.error(f"Ошибка синхронизации: {e}")


def run_sync():
    settings = read_config("config.ini")
    try:
        validate_settings(settings)
    except ValueError as err:
        print(f"Ошибка запуска: {err}")
        return

    logger = setup_logger(settings["log_file"])
    local_folder = Path(settings["local_folder"])
    storage = YandexDiskStorage(settings["token"], settings["cloud_folder_name"])
    logger.info(f"Сервис запущен. Синхронизируемая папка: {local_folder}")

    sync_interval = int(settings["sync_interval"])
    while True:
        sync(local_folder, storage, logger)
        time.sleep(sync_interval)
