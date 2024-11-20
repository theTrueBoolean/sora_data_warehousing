BEGIN;

-- Create Raw, Staging, and Prod Schemas
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS prod;

-- Create Raw table for Float data
CREATE TABLE IF NOT EXISTS raw.float_allocations (
    client TEXT,
    project TEXT,
    role TEXT,
    name TEXT,
    task TEXT,
    start_date DATE,
    end_date DATE,
    estimated_hours INTEGER
);

-- Create Raw table for ClickUp data
CREATE TABLE IF NOT EXISTS raw.clickup_timesheets (
    client TEXT,
    project TEXT,
    name TEXT,
    task TEXT,
    date DATE,
    hours FLOAT,
    note TEXT,
    billable TEXT
);

COMMIT;