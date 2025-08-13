import os
import time
from dotenv import load_dotenv
from loguru import logger
from cloud_storage import YandexDiskStorage

# Загрузка переменных окружения
load_dotenv()
SYNC_FOLDER = os.getenv("SYNC_FOLDER")
CLOUD_FOLDER = os.getenv("CLOUD_FOLDER")
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
SYNC_INTERVAL = int(os.getenv("SYNC_INTERVAL", 60))
LOG_FILE = os.getenv("LOG_FILE", "sync.log")

# Настройка логгера
logger.add(LOG_FILE, rotation="10 MB", level="INFO")


def validate_environment():
    """Проверка корректности параметров"""
    errors = []

    if not os.path.isdir(SYNC_FOLDER):
        errors.append(f"Папка синхронизации не существует: {SYNC_FOLDER}")
    if not ACCESS_TOKEN:
        errors.append("Токен доступа не указан")
    if SYNC_INTERVAL < 10:
        errors.append("Интервал синхронизации должен быть >= 10 секунд")

    if errors:
        for error in errors:
            logger.error(error)
        raise EnvironmentError("Ошибка конфигурации")


def get_local_files() -> dict:
    """Сканирование локальной папки: {имя_файла: время_изменения}"""
    files = {}
    for item in os.listdir(SYNC_FOLDER):
        # Игнорируем файлы, начинающиеся с точки (скрытые)
        if item.startswith("."):
            continue

        item_path = os.path.join(SYNC_FOLDER, item)
        if os.path.isfile(item_path):
            files[item] = os.path.getmtime(item_path)
    return files


def full_sync(storage, local_files: dict):
    """Первоначальная полная синхронизация"""
    cloud_files = storage.get_info()
    local_names = set(local_files.keys())

    # Удаление лишних файлов в облаке
    for filename in cloud_files - local_names:
        try:
            storage.delete(filename)
            logger.info(f"Удален в облаке: {filename}")
        except Exception as e:
            logger.error(f"Ошибка удаления {filename}: {str(e)}")

    # Загрузка новых/обновление существующих файлов
    for filename in local_names:
        local_path = os.path.join(SYNC_FOLDER, filename)
        try:
            if filename in cloud_files:
                storage.reload(local_path)
                logger.info(f"Обновлен: {filename}")
            else:
                storage.load(local_path)
                logger.info(f"Загружен: {filename}")
        except Exception as e:
            logger.error(f"Ошибка синхронизации {filename}: {str(e)}")


def incremental_sync(storage, prev_state: dict, current_state: dict):
    """Инкрементальная синхронизация изменений"""
    prev_files = set(prev_state.keys())
    current_files = set(current_state.keys())

    # Удаление файлов, отсутствующих локально
    for filename in prev_files - current_files:
        try:
            storage.delete(filename)
            logger.info(f"Удален: {filename}")
        except Exception as e:
            logger.error(f"Ошибка удаления {filename}: {str(e)}")

    # Получение текущего состояния облака
    try:
        cloud_files = storage.get_info()
    except Exception as e:
        logger.error(f"Ошибка получения информации из облака: {str(e)}")
        return

    # Обработка всех локальных файлов за один проход
    for filename in current_files:
        local_path = os.path.join(SYNC_FOLDER, filename)

        # Файл новый (отсутствует в предыдущем состоянии)
        if filename not in prev_state:
            try:
                storage.load(local_path)
                logger.info(f"Загружен: {filename}")
            except Exception as e:
                logger.error(f"Ошибка загрузки {filename}: {str(e)}")

        # Файл изменен (присутствует в предыдущем состоянии и изменился)
        elif prev_state[filename] != current_state[filename]:
            try:
                storage.reload(local_path)
                logger.info(f"Обновлен: {filename}")
            except Exception as e:
                logger.error(f"Ошибка обновления {filename}: {str(e)}")

        # Файл не изменился локально, но отсутствует в облаке
        elif filename not in cloud_files:
            try:
                storage.load(local_path)
                logger.info(f"Восстановлен: {filename}")
            except Exception as e:
                logger.error(f"Ошибка восстановления {filename}: {str(e)}")


def main():
    """Основной цикл синхронизации"""
    validate_environment()
    logger.info(f"Сервис запущен. Синхронизация: {SYNC_FOLDER}")

    storage = YandexDiskStorage(ACCESS_TOKEN, CLOUD_FOLDER)
    prev_state = get_local_files()

    # Первоначальная синхронизация
    full_sync(storage, prev_state)

    # Основной цикл
    while True:
        time.sleep(SYNC_INTERVAL)
        current_state = get_local_files()
        incremental_sync(storage, prev_state, current_state)
        prev_state = current_state


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.critical(f"Критическая ошибка: {str(e)}")
        raise
