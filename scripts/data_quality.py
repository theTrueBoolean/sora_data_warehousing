from loguru import logger
from sqlalchemy import text
from utils.db import engine as db_engine

def data_quality_checks_raw(engine=db_engine):
    logger.info("Running data quality checks on raw schema...")

    def check_missing_values(engine):
        # check for missing valus in critical fields
        critical_checks = [
            ("raw.float_allocations", "name"),
            ("raw.float_allocations", "project"),
            ("raw.clickup_timesheets", "project"),
            ("raw.clickup_timesheets", "name"),
            ("raw.clickup_timesheets", "date"),
        ]

        for table, column in critical_checks:
            query = text(f"SELECT COUNT(*) FROM {table} WHERE {column} IS NULL")
            result = engine.execute(query).fetchone()
            if result[0] > 0:
                logger.error(f"Data quality check failed: Found {result[0]} missing values in {column} of table {table}")
                raise
            else:
                logger.info(f"Data quality check passed: No missing values in {column} of table {table}")

    def check_unique_constraints(engine):
        # Check for unique constraints (such as combination of client, team member, project, and date)
        unique_check_query = """
        SELECT client, name, project, task, date, COUNT(*)
        FROM raw.clickup_timesheets
        GROUP BY client, name, project, task, date
        HAVING COUNT(*) > 1
        """
        result = engine.execute(text(unique_check_query)).fetchall()
        if result:
            logger.error(f"Data quality check failed: Duplicate entries found in ClickUp timesheets for client/name/project/date combination")
            raise
        else:
            logger.info("Data quality check passed: No duplicate entries in ClickUp timesheets for name/project/date")

    def check_referential_integrity(engine):
        # Check referential integrity between Float and ClickUp data
        referential_query = """
        SELECT DISTINCT name
        FROM raw.clickup_timesheets
        WHERE name NOT IN (SELECT DISTINCT name FROM raw.float_allocations)
        """
        result = engine.execute(text(referential_query)).fetchall()
        if result:
            logger.error(f"Data quality check failed: {len(result)} team members found in ClickUp that do not exist in Float Allocations")
            raise
        else:
            logger.info("Data quality check passed: Referential integrity between ClickUp and Float data is maintained")

    def check_numeric_ranges(engine):
        # Validate ranges for numeric columns
        range_checks = [
            ("raw.float_allocations", "estimated_hours", "estimated_hours >= 0"),
            ("raw.clickup_timesheets", "hours", "hours >= 0")
        ]

        for table, column, condition in range_checks:
            query = text(f"SELECT COUNT(*) FROM {table} WHERE NOT ({condition})")
            result = engine.execute(query).fetchone()
            if result[0] > 0:
                logger.error(f"Data quality check failed: {result[0]} invalid values in {column} of table {table}")
                raise
            else:
                logger.info(f"Data quality check passed: All values in {column} of table {table} are within the valid range")

    check_missing_values(engine)
    check_unique_constraints(engine)
    check_referential_integrity(engine)
    check_numeric_ranges(engine)

def data_quality_checks_staging(engine=db_engine):
    """
    Run a series of data quality checks on the data in the staging schema.
    Ensures data integrity before migration to the production schema.
    """
    logger.info("Running data quality checks on the staging star schema...")

    def run_checks(engine, checks):
        # Execute each check and log the results
        with engine.connect() as conn:
            for query, error_message in checks:
                result = conn.execute(text(query)).fetchone()
                if result[0] > 0:
                    logger.error(f"Data quality check failed: {error_message} (Count: {result[0]})")
                    raise
                else:
                    logger.info(f"Data quality check passed: {error_message}")

    checks = [
        # Null Value Checks
        ("SELECT COUNT(*) FROM staging.fact_timesheet WHERE date_id IS NULL", "Null values found in 'date_id' column of 'fact_timesheet'"),
        ("SELECT COUNT(*) FROM staging.fact_timesheet WHERE team_member_id IS NULL", "Null values found in 'team_member_id' column of 'fact_timesheet'"),
        ("SELECT COUNT(*) FROM staging.fact_timesheet WHERE project_id IS NULL", "Null values found in 'project_id' column of 'fact_timesheet'"),
        ("SELECT COUNT(*) FROM staging.fact_timesheet WHERE task_id IS NULL", "Null values found in 'task_id' column of 'fact_timesheet'"),

        # Duplicate Checks in Dimension Tables
        ("SELECT COUNT(name) - COUNT(DISTINCT name) FROM staging.dim_team_member", "Duplicate entries found in 'dim_team_member' table"),
        ("SELECT COUNT(project_name) - COUNT(DISTINCT project_name) FROM staging.dim_project", "Duplicate entries found in 'dim_project' table"),
        ("SELECT COUNT(task_name) - COUNT(DISTINCT task_name) FROM staging.dim_task", "Duplicate entries found in 'dim_task' table"),

        # Referential Integrity Checks
        ("""
        SELECT COUNT(*)
        FROM staging.fact_timesheet ft
        LEFT JOIN staging.dim_team_member tm ON ft.team_member_id = tm.team_member_id
        WHERE tm.team_member_id IS NULL
        """, "Referential integrity check failed: 'team_member_id' in 'fact_timesheet' not found in 'dim_team_member'"),

        ("""
        SELECT COUNT(*)
        FROM staging.fact_timesheet ft
        LEFT JOIN staging.dim_project dp ON ft.project_id = dp.project_id
        WHERE dp.project_id IS NULL
        """, "Referential integrity check failed: 'project_id' in 'fact_timesheet' not found in 'dim_project'"),

        ("""
        SELECT COUNT(*)
        FROM staging.fact_timesheet ft
        LEFT JOIN staging.dim_task dt ON ft.task_id = dt.task_id
        WHERE dt.task_id IS NULL
        """, "Referential integrity check failed: 'task_id' in 'fact_timesheet' not found in 'dim_task'"),

        # Range and Outlier Checks
        ("SELECT COUNT(*) FROM staging.fact_timesheet WHERE log_hours < 0", "Negative values found in 'log_hours' column of 'fact_timesheet'"),
        ("SELECT COUNT(*) FROM staging.fact_timesheet WHERE est_project_hours < 0", "Negative values found in 'est_project_hours' column of 'fact_timesheet'"),
        ("SELECT COUNT(*) FROM staging.fact_timesheet WHERE log_hours > 1000", "Unrealistically high values found in 'log_hours' column of 'fact_timesheet'"),

        # Consistency Checks
        ("""
        SELECT COUNT(*)
        FROM staging.fact_timesheet
        WHERE log_hours > est_project_hours
        """, "Inconsistent data: 'log_hours' exceeds 'estimated_hours' in 'fact_timesheet'")
    ]

    run_checks(engine, checks)

def run_prod_validation(engine=db_engine):
    """
    Perform final validation checks on the production schema
    to ensure data integrity before marking the pipeline as complete.
    """
    logger.info("Running final validation checks on the production schema...")

    def run_validation_checks(engine, validation_checks):
        with engine.connect() as conn:
            for query, validation_message in validation_checks:
                result = conn.execute(text(query)).fetchone()
                row_count = result[0]
                if "Row count check" in validation_message and row_count == 0:
                    logger.error(f"Final validation check failed: {validation_message} (Row count: {row_count})")
                    raise ValueError(f"Final validation check failed: {validation_message} (Row count: {row_count})")
                elif "Row count check" not in validation_message and row_count != 0:
                    logger.error(f"Final validation check failed: {validation_message} (Discrepancy: {row_count})")
                    raise
                else:
                    logger.info(f"Validation check passed: {validation_message}")

    def check_data_types(engine, expected_data_types):
        # data type checks
        for table, columns in expected_data_types.items():
            logger.info(f"Performing data type checks for table: {table}")
            table_name = table.split('.')[-1]
            for column, expected_type in columns.items():
                type_query = f"""
                        SELECT data_type
                        FROM information_schema.columns
                        WHERE table_schema = '{table.split('.')[0]}'
                          AND table_name = '{table_name}'
                          AND column_name = '{column}';
                        """
                with engine.connect() as conn:
                    actual_type_result = conn.execute(text(type_query)).fetchone()
                    if actual_type_result is None:
                        logger.error(f"Column '{column}' not found in '{table}'")
                        raise ValueError(f"Column '{column}' not found in '{table}'")

                    actual_type = actual_type_result[0]
                    if actual_type != expected_type:
                        logger.error(
                            f"Data type mismatch for column '{column}' in '{table}': expected '{expected_type}', found '{actual_type}'")
                        raise TypeError(
                            f"Data type mismatch for column '{column}' in '{table}': expected '{expected_type}', found '{actual_type}'")
                    else:
                        logger.info(f"Data type check passed for column '{column}' in '{table}': '{actual_type}'")

    validation_checks = [
        # row count verification
        ("SELECT COUNT(*) FROM prod.dim_team_member", "Row count check for 'dim_team_member' table"),
        ("SELECT COUNT(*) FROM prod.dim_project", "Row count check for 'dim_project' table"),
        ("SELECT COUNT(*) FROM prod.dim_task", "Row count check for 'dim_task' table"),
        ("SELECT COUNT(*) FROM prod.fact_timesheet", "Row count check for 'fact_timesheet' table"),

        # Null value checks
        ("SELECT COUNT(*) FROM prod.fact_timesheet WHERE team_member_id IS NULL", "Null values check in 'team_member_id' column"),
        ("SELECT COUNT(*) FROM prod.fact_timesheet WHERE project_id IS NULL", "Null values check in 'project_id' column"),
        ("SELECT COUNT(*) FROM prod.fact_timesheet WHERE task_id IS NULL", "Null values check in 'task_id' column"),
        ("SELECT COUNT(*) FROM prod.fact_timesheet WHERE date_id IS NULL", "Null values check in 'date_id' column"),

        # data integrity checks
        ("""
        SELECT COUNT(*)
        FROM prod.fact_timesheet ft
        LEFT JOIN prod.dim_team_member tm ON ft.team_member_id = tm.team_member_id
        WHERE tm.team_member_id IS NULL
        """, "Foreign key relationship for 'team_member_id' in 'fact_timesheet'"),

        ("""
        SELECT COUNT(*)
        FROM prod.fact_timesheet ft
        LEFT JOIN prod.dim_project dp ON ft.project_id = dp.project_id
        WHERE dp.project_id IS NULL
        """, "Foreign key relationship for 'project_id' in 'fact_timesheet'"),

        ("""
        SELECT COUNT(*)
        FROM prod.fact_timesheet ft
        LEFT JOIN prod.dim_task dt ON ft.task_id = dt.task_id
        WHERE dt.task_id IS NULL
        """, "Foreign key relationship for 'task_id' in 'fact_timesheet'")
    ]

    expected_data_types = {
        "prod.dim_date": {
            "date_id": "integer",
            "date": "date",
            "day_of_week": "text",
            "day": "integer",
            "month": "integer",
            "year": "integer",
            "is_weekend": "boolean"
        },
        "prod.dim_team_member": {
            "team_member_id": "integer",
            "name": "text",
            "role": "text",
        },
        "prod.dim_project": {
            "project_id": "integer",
            "project_name": "text",
            "client": "text",
        },
        "prod.dim_task": {
            "task_id": "integer",
            "task_name": "text",
        },
        "prod.fact_timesheet": {
            "date_id": "integer",
            "team_member_id": "integer",
            "project_id": "integer",
            "task_id": "integer",
            "log_hours": "double precision",
            "est_project_hours": "integer",
            "is_billable": "boolean",
        }
    }

    run_validation_checks(engine, validation_checks)
    check_data_types(engine, expected_data_types)

    logger.info("All data quality checks passed successfully.")