BEGIN;

-- Create Prod Tables with Constraints and Indexes
CREATE TABLE IF NOT EXISTS prod.dim_date (LIKE staging.dim_date INCLUDING ALL);
CREATE TABLE IF NOT EXISTS prod.dim_team_member (LIKE staging.dim_team_member INCLUDING ALL);
CREATE TABLE IF NOT EXISTS prod.dim_project (LIKE staging.dim_project INCLUDING ALL);
CREATE TABLE IF NOT EXISTS prod.dim_task (LIKE staging.dim_task INCLUDING ALL);
CREATE TABLE IF NOT EXISTS prod.fact_timesheet (LIKE staging.fact_timesheet INCLUDING ALL);

-- Insert Data into Prod Tables
INSERT INTO prod.dim_date SELECT * FROM staging.dim_date;
INSERT INTO prod.dim_team_member SELECT * FROM staging.dim_team_member;
INSERT INTO prod.dim_project SELECT * FROM staging.dim_project;
INSERT INTO prod.dim_task SELECT * FROM staging.dim_task;
INSERT INTO prod.fact_timesheet SELECT * FROM staging.fact_timesheet;

-- Add Foreign Key Constraints to Fact Tables
ALTER TABLE prod.fact_timesheet
    ADD CONSTRAINT fk_fact_timesheet_date_id FOREIGN KEY (date_id) REFERENCES prod.dim_date(date_id),
    ADD CONSTRAINT fk_fact_timesheet_team_member_id FOREIGN KEY (team_member_id) REFERENCES prod.dim_team_member(team_member_id),
    ADD CONSTRAINT fk_fact_timesheet_project_id FOREIGN KEY (project_id) REFERENCES prod.dim_project(project_id),
    ADD CONSTRAINT fk_fact_timesheet_task_id FOREIGN KEY (task_id) REFERENCES prod.dim_task(task_id);

-- create Indexes for Dimension tables
CREATE INDEX idx_dim_date_date ON prod.dim_date(date);
CREATE INDEX idx_dim_team_member_name ON prod.dim_team_member(name);
CREATE INDEX idx_dim_project_client ON prod.dim_project(client);
CREATE INDEX idx_dim_project_project_name ON prod.dim_project(project_name);


-- Create Indexes for Fact tables
CREATE INDEX idx_fact_timesheet_team_member_id ON prod.fact_timesheet(team_member_id);
CREATE INDEX idx_fact_timesheet_project_id ON prod.fact_timesheet(project_id);
CREATE INDEX idx_fact_timesheet_task_id ON prod.fact_timesheet(task_id);
CREATE INDEX idx_fact_timesheet_date_id ON prod.fact_timesheet(date_id);
CREATE INDEX idx_fact_timesheet_is_billable ON prod.fact_timesheet(is_billable);

-- -- Composite index
-- CREATE INDEX idx_fact_timesheet_project_task_date ON prod.fact_timesheet(project_id, task_id, date_id);

COMMIT;