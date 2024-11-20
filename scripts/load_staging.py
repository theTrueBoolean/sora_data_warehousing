
from sqlalchemy import text
from utils.db import engine
from loguru import logger

def create_and_refresh_materialized_views():
    logger.info("Creating and refreshing materialized views in the staging schema")
    try:
        with engine.begin() as conn:
            view_creation_script = open("sql/create_staging_views.sql", "r").read()
            conn.execute(text(view_creation_script))
            logger.info("Materialized views created and refreshed successfully")
    except Exception as e:
        logger.error(f"Error creating or refreshing materialized views: {e}")
        raise e

