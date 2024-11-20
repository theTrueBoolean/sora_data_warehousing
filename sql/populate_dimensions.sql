BEGIN;
-- Populate Date Dimension
INSERT INTO staging.dim_date (date, day_of_week, day, month, year, is_weekend)
SELECT DISTINCT
    date,
    TO_CHAR(date, 'Day') AS day_of_week,
    EXTRACT(DAY FROM date) AS day,
    EXTRACT(MONTH FROM date) AS month,
    EXTRACT(YEAR FROM date) AS year,
    CASE WHEN EXTRACT(DOW FROM date) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend
FROM staging.mv_clickup_timesheets;

-- Populate Team Member Dimension
INSERT INTO staging.dim_team_member (name, role)
SELECT DISTINCT
    LOWER(name) AS name,
    role
FROM staging.mv_float_allocations;

-- Populate Project Dimension
INSERT INTO staging.dim_project (client, project_name)
SELECT DISTINCT
    client,
    project
FROM staging.mv_clickup_timesheets;

-- Populate Task Dimension
INSERT INTO staging.dim_task (task_name)
SELECT DISTINCT
    task
FROM staging.mv_clickup_timesheets
;

COMMIT;