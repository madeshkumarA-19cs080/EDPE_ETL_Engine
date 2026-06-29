import sys
from pathlib import Path

from loguru import logger

from config import LOG_DIR, LOG_FILE

LOG_DIR.mkdir(parents=True, exist_ok=True)

logger.remove()
logger.add(
    sys.stdout,
    level="INFO",
    format="[EDPE] {time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
)
logger.add(
    str(LOG_FILE),
    rotation="10 MB",
    retention="10 days",
    level="DEBUG",
    enqueue=True,
    backtrace=True,
    diagnose=True,
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
)
