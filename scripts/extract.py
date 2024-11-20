
import os
import pandas as pd
from loguru import logger
from models.data_models import FloatDataModel, ClickUpDataModel
from typing import List


def extract_and_validate(file_path: str, model):
    logger.info(f"Extracting data from {file_path}")

    if not os.path.exists(file_path):
        logger.error(f"File not found: {file_path}")
        raise FileNotFoundError

    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        logger.error(f"Unable to read CSV file {file_path}: {e}")
        raise e

    def validate_row(row):
        try:
            return model(**row.to_dict()).dict()
        except Exception as e:
            logger.error(f"Data validation error at row {row.name}: {e}")


    validated_data = df.apply(validate_row, axis=1).dropna()

    if validated_data.empty:
        logger.warning(f"No valid data extracted from {file_path}")
        return None

    logger.info(f"Extracted {len(validated_data)} valid data rows from {file_path}")
    return pd.json_normalize(validated_data.reset_index(drop=True).tolist())


