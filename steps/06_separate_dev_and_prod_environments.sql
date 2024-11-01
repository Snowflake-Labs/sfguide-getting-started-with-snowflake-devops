/*
----------------------------------------------------------------------------------
Perform the following changes to parametrize the deployment target of the pipeline
----------------------------------------------------------------------------------

-- Parametrize the database name of the CREATE DATABASE command in steps/01_setup_snowflake.sql
CREATE OR ALTER DATABASE QUICKSTART_{{environment}}; 


-- Use the environment variable in steps/03_harmonize_data.py
silver_schema = root.databases[f"quickstart_{os.environ['environment']}"].schemas["silver"]


-- Parametrize the USE SCHEMA in steps/04_orchestrate_jobs.sql
use schema quickstart_{{environment}}.gold;


-- Parametrize DATA_RETENTION_TIME_IN_DAYS of CREATE OR ALTER TABLE in steps/04_orchestrate_jobs.sql
data_retention_time_in_days = {{retention_time}};
*/
