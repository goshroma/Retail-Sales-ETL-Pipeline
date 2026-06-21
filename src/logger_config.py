"""
logger_config.py
------------------
Centralized logging setup for the whole pipeline.

Every stage module gets its own logger via
`logging.getLogger("retail_etl.<stage>")` (e.g. "retail_etl.ingestion",
"retail_etl.cleaning"). Those are all children of the "retail_etl"
logger, so configuring handlers ONCE on "retail_etl" here -- rather than
each module configuring logging for itself -- is enough to make every
stage's log lines show up consistently in both the console and
logs/pipeline.log. main.py calls `setup_logging()` exactly once, before
any stage runs.
"""

import logging
import os

ROOT_LOGGER_NAME = "retail_etl"


def setup_logging(log_dir: str = "logs", log_filename: str = "pipeline.log", level: int = logging.INFO) -> None:
    """Configures console + rotating-by-run file handlers on the retail_etl logger tree."""
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, log_filename)

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    file_handler = logging.FileHandler(log_path, mode="a", encoding="utf-8")
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    root_logger = logging.getLogger(ROOT_LOGGER_NAME)
    root_logger.setLevel(level)
    root_logger.handlers.clear()  # idempotent: safe if setup_logging() is ever called more than once
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    # Stop here rather than also bubbling up to Python's default root
    # logger, which would otherwise print every line a second time.
    root_logger.propagate = False

    root_logger.info("Logging initialized -> %s", log_path)
