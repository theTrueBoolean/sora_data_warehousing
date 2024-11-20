BEGIN;

-- Create Materalized View for Float Data
CREATE MATERIALIZED VIEW IF NOT EXISTS staging.mv_float_allocations AS
SELECT
    client,
    project,
    role,
    LOWER(name) AS name,
    LOWER(task) AS task,
    CAST(start_date AS DATE) AS start_date,
    CAST(end_date AS DATE) AS end_date,
    CAST(COALESCE(estimated_hours,0) AS INTEGER) AS est_project_hours
FROM
    raw.float_allocations;

-- Create Materialized View for ClickUp Data
CREATE MATERIALIZED VIEW IF NOT EXISTS staging.mv_clickup_timesheets AS
SELECT
    client,
    project,
    LOWER(name) AS name,
    LOWER(task) AS task,
    CAST(date AS DATE) AS date,
    CAST(hours AS FLOAT) AS log_hours,
    CAST(
        CASE
            WHEN billable ILIKE 'Yes' THEN TRUE
            ELSE FALSE
        END
    AS BOOLEAN) AS is_billable
FROM
    raw.clickup_timesheets;

-- Refresh the materialized views
REFRESH MATERIALIZED VIEW staging.mv_float_allocations;
REFRESH MATERIALIZED VIEW staging.mv_clickup_timesheets;

COMMIT;
