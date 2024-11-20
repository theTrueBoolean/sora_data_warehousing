
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from utils.db import engine
from loguru import logger

def create_raw_schema():
    logger.info("Creating schemas and raw tables")
    with engine.begin() as conn:
        schema_creation_script = open("sql/create_raw_schema.sql", "r").read()
        conn.execute(text(schema_creation_script))
        logger.info("Schemas and tables created successfully")

def load_to_raw(df: pd.DataFrame, table_name: str):
    logger.info(f"Loading data into raw table: {table_name}")
    try:
        df.to_sql(table_name, con=engine, schema="raw", if_exists="replace", index=False)
        logger.info(f"Successfully loaded data into {table_name}")
    except SQLAlchemyError as e:
        logger.error(f"SQLAlchemy error: {e}")
        raise e
    except Exception as e:
        logger.error(f"Unexpected error loading data into {table_name}: {e}")
        raise e
