import logging
import os
from producer.config import LOGGING_CONFIG

os.makedirs("logs", exist_ok=True)


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(LOGGING_CONFIG["level"])

    if not logger.handlers:
        formatter = logging.Formatter(LOGGING_CONFIG["format"])

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        file_handler = logging.FileHandler(LOGGING_CONFIG["file"])
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger
