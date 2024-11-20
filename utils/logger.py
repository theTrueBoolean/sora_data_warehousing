# utils/logger.py

from loguru import logger

logger.add("logs/data_pipeline.log", rotation="1 MB", retention="7 days", level="INFO")
