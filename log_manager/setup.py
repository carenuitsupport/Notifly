# log_manager/setup.py
import logging
import logging.config
import os
import json
from pathlib import Path

_initialized = False

def setup_logging() -> None:
    global _initialized
    if _initialized:
        return

    current_dir = os.path.dirname(os.path.abspath(__file__))
    config_file = Path(os.path.join(current_dir, "logging_configs", "config_prod.json"))

    try:
        with open(config_file, "r") as f:
            config = json.load(f)
        logging.config.dictConfig(config)
        _initialized = True
    except Exception as e:
        # Fallback so you still see something on the console
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
        logging.getLogger(__name__).exception("Failed to load logging config; using basicConfig. Error: %s", e)

def get_logger(name: str) -> logging.Logger:
    # Ensure logging is initialized even if caller forgot
    if not _initialized:
        setup_logging()
    return logging.getLogger(name)
