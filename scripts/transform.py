
from loguru import logger
import pandas as pd

from star_schema import create_staging_star_schema, populate_dimensions, populate_fact_table, load_prod_schema

#TODO: modularize data cleaning
def clean_data(df: pd.DataFrame, dataset_name: str) -> pd.DataFrame:
    logger.info(f"Starting data cleaning for dataset: {dataset_name}")

    # Drop duplicates
    initial_count = len(df)

    df.drop_duplicates(inplace=True)
    logger.warning(f"{initial_count - len(df)} duplicate rows removed from {dataset_name}")
    curr_count = len(df)

    # Drop rows with critical missing values
    #TODO: move critical columns and other data quality thresholds to config
    critical_columns = ["client", "project", "name", "task"]
    df.dropna(subset=critical_columns, inplace=True)
    logger.warning(f"{curr_count - len(df)} rows with missing critical fields removed from: {dataset_name}")
    curr_count = len(df)

    # handle missing values in non-critical columns
    if "note" in df.columns:
        df["note"].fillna("", inplace=True)

    if "billable" in df.columns:
        df["billable"].fillna("No", inplace=True)

    # Ensure date columns are in correct datetime format
    date_columns = ["start_date", "end_date", "date"]
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
            invalid_dates = df[col].isnull().sum()
            if invalid_dates > 0:
                logger.warning(
                    f"Found {invalid_dates} invalid dates in column {col} of {dataset_name}, removing invalid rows")
            df = df[df[col].notna()]
    logger.warning(f"Removed {curr_count - len(df)} invalid date rows from {dataset_name}")
    curr_count = len(df)

    if "hours" in df.columns:
        df = df[df["hours"] >= 0]
        logger.warning(f"{curr_count - len(df)} negative values in 'hours' column removed from {dataset_name}")
        curr_count = len(df)

    # clean text columns
    if "role" in df.columns:
        df["role"] = df["role"].str.lower().str.strip()

    if "task" in df.columns:
        df["task"] = df["task"].str.lower().str.strip()

    if "billable" in df.columns:
        df["billable"] = df["billable"].str.lower().str.strip()

    if "client" in df.columns:
        df["client"] = df["client"].str.lower().str.strip()

    logger.info(f"Finished data cleaning for dataset: {dataset_name}")
    logger.info(f"{dataset_name} now has {curr_count} rows.")
    return df

def star_schema_transformation():
    create_staging_star_schema()
    populate_dimensions()
    populate_fact_table()
    load_prod_schema()
