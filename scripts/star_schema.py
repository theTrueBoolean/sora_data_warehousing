
from sqlalchemy import text
from utils.db import engine
from loguru import logger

def create_staging_star_schema():
    logger.info("Creating star schema tables in the staging schema")
    try:
        with engine.begin() as conn:
            star_schema_script = open("sql/create_staging_star_schema.sql", "r").read()
            conn.execute(text(star_schema_script))
            logger.info("Star schema tables created successfully")
    except Exception as e:
        logger.error(f"Error creating star schema tables: {e}")
        raise e

def load_prod_schema():
    logger.info("Creating and loading tables in the prod schema")
    try:
        with engine.begin() as conn:
            star_schema_script = open("sql/load_prod_schema.sql", "r").read()
            conn.execute(text(star_schema_script))
            logger.info("Prod tables star schema loaded successfully!")
    except Exception as e:
        logger.error(f"Error loading prod star schema tables: {e}")
        raise e

def populate_dimensions():
    logger.info("Populating dimension tables")
    try:
        with engine.begin() as conn:
            dimension_script = open("sql/populate_dimensions.sql", "r").read()
            conn.execute(text(dimension_script))
            logger.info("Dimension tables populated successfully")
    except Exception as e:
        logger.error(f"Error populating dimension tables: {e}")
        raise e

def populate_fact_table():
    logger.info("Populating fact table")
    try:
        with engine.begin() as conn:
            fact_script = open("sql/populate_fact_table.sql", "r").read()
            conn.execute(text(fact_script))
            logger.info("Fact table populated successfully")
    except Exception as e:
        logger.error(f"Error populating fact table: {e}")
        raise e
