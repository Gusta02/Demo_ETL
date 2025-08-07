import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from datetime import datetime

def setup_logger(name="pipeline"):
    logs_dir = Path(__file__).resolve().parent.parent / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    log_file = logs_dir / f"{name}_pipeline_{datetime.now():%Y%m%d}.log"

    handler = RotatingFileHandler(log_file, maxBytes=5 * 1024 * 1024, backupCount=5, encoding="utf-8")
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[handler, logging.StreamHandler()]
    )
    return logging.getLogger(name)
