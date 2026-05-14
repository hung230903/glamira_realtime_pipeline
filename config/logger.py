import logging
from logging.handlers import RotatingFileHandler
import os

LOG_DIR = os.getenv("APP_LOG_DIR", "/tmp/logs")
LOG_FILE = "app.log"
LOG_LEVEL = logging.INFO
LOG_MAX_BYTES = 5_000_000

LOG_BACKUP_COUNT = 3


def setup_logger(name=None, log_folder=None, log_file=LOG_FILE):
    logger = logging.getLogger(name)
    logger.setLevel(LOG_LEVEL)

    # Avoid adding multiple handlers if the logger already has them
    if logger.hasHandlers():
        return logger

    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    if log_folder:
        log_dir = os.path.join(LOG_DIR, log_folder)
    else:
        log_dir = LOG_DIR

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    try:
        os.makedirs(log_dir, exist_ok=True)
        file_path = os.path.join(log_dir, log_file)
        file_handler = RotatingFileHandler(
            file_path,
            maxBytes=LOG_MAX_BYTES,
            backupCount=LOG_BACKUP_COUNT,
            encoding="utf-8",
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    except PermissionError:
        logger.warning(f"Permission denied for log file at {log_dir}. Falling back to console-only logging.")
    except Exception as e:
        logger.warning(f"Could not initialize file logging: {e}. Falling back to console-only logging.")

    return logger
