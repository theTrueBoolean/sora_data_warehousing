
BEGIN;

-- Create Date Dimension Table
CREATE TABLE IF NOT EXISTS staging.dim_date (
    date_id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    day_of_week TEXT,
    day INTEGER,
    month INTEGER,
    year INTEGER,
    is_weekend BOOLEAN
);

-- Create Team Member Dimension Table
CREATE TABLE IF NOT EXISTS staging.dim_team_member (
    team_member_id SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    role TEXT NOT NULL
);

-- Create Project Dimension Table
CREATE TABLE IF NOT EXISTS staging.dim_project (
    project_id SERIAL PRIMARY KEY,
    client TEXT NOT NULL,
    project_name TEXT NOT NULL,
    UNIQUE(client, project_name)
);

-- Create Task Dimension Table
CREATE TABLE IF NOT EXISTS staging.dim_task (
    task_id SERIAL PRIMARY KEY,
    task_name TEXT UNIQUE NOT NULL
);

-- Create Fact Timesheet Table
CREATE TABLE IF NOT EXISTS staging.fact_timesheet (
    timesheet_id SERIAL PRIMARY KEY,
    date_id INTEGER REFERENCES staging.dim_date(date_id),
    team_member_id INTEGER REFERENCES staging.dim_team_member(team_member_id),
    project_id INTEGER REFERENCES staging.dim_project(project_id),
    task_id INTEGER REFERENCES staging.dim_task(task_id),
    log_hours FLOAT,
    est_project_hours INTEGER,
    is_billable BOOLEAN DEFAULT FALSE,
    FOREIGN KEY (team_member_id) REFERENCES staging.dim_team_member(team_member_id),
    FOREIGN KEY (project_id) REFERENCES staging.dim_project(project_id),
    FOREIGN KEY (task_id) REFERENCES staging.dim_task(task_id),
    FOREIGN KEY (date_id) REFERENCES staging.dim_date(date_id)
);
COMMIT;