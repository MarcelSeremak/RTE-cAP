import logging
import os
import datetime

def get_logger(name):
    log_dir = os.path.join("logs", name)
    os.makedirs(log_dir, exist_ok=True)

    log_filename = os.path.join(log_dir, f"{datetime.datetime.now().date()}.log")
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        fileHandler = logging.FileHandler(log_filename, encoding="utf-8")
        consoleHandler = logging.StreamHandler()

        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        fileHandler.setFormatter(formatter)
        consoleHandler.setFormatter(formatter)
        logger.addHandler(fileHandler)
        logger.addHandler(consoleHandler)

        logger.propagate = False

    return logger