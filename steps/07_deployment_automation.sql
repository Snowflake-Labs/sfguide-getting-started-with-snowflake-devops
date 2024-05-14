/*
-----------------------------------------------------------------------------------------------
Commit and push your changes from 06_separate_dev_and_prod_environments.sql to the "dev" branch
This will start the preconfigured CI/CD pipeline and deploy the changes
-----------------------------------------------------------------------------------------------
*/

use role accountadmin;

-- should return 0 (due to deploy_pipeline_dev.yml)
show parameters like 'data_retention_time_in_days' in table quickstart_dev.gold.vacation_spots;
-- should return 1 (due to deploy_pipeline_dev.yml)
show parameters like 'data_retention_time_in_days' in table quickstart_prod.gold.vacation_spots;
