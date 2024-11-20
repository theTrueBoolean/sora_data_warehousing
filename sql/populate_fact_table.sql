BEGIN;

-- Populate Fact Timesheet Table
INSERT INTO staging.fact_timesheet (date_id, team_member_id, project_id, task_id, log_hours, est_project_hours, is_billable)
SELECT
    d.date_id,
    tm.team_member_id,
    p.project_id,
    t.task_id,
    cu.log_hours,
    fa.est_project_hours,
    cu.is_billable
FROM staging.mv_clickup_timesheets cu
LEFT JOIN staging.mv_float_allocations fa
    ON cu.name = fa.name
    AND cu.client = fa.client
    AND cu.project = fa.project
    AND cu.task = fa.task
JOIN staging.dim_date d
    ON cu.date = d.date
JOIN staging.dim_team_member tm
    ON cu.name = tm.name
    AND fa.role = tm.role
JOIN staging.dim_project p
    ON cu.project = p.project_name
    AND cu.client = p.client
JOIN staging.dim_task t
    ON cu.task = t.task_name
;

COMMIT;