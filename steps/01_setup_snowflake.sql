USE ROLE ACCOUNTADMIN;



-- Separate database for git repository
CREATE OR ALTER DATABASE GIT;


-- API integration is needed for GitHub integration
CREATE OR REPLACE API INTEGRATION git_api_integration
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('https://github.com/LaurentiuC1976') -- INSERT YOUR GITHUB USERNAME HERE
  ENABLED = TRUE;


-- Git repository object is similar to external stage
CREATE OR REPLACE GIT REPOSITORY GIT.public.git_repo
  API_INTEGRATION = git_api_integration
  ORIGIN = 'https://github.com/LaurentiuC1976/DPF_REP'; -- INSERT URL OF FORKED REPO HERE


CREATE OR ALTER DATABASE DEMO_PROD; 


-- To monitor data pipeline's completion
/*CREATE OR REPLACE NOTIFICATION INTEGRATION email_integration
  TYPE=EMAIL
  ENABLED=TRUE;


-- Database level objects
CREATE OR ALTER SCHEMA bronze;
CREATE OR ALTER SCHEMA silver;
CREATE OR ALTER SCHEMA gold;*/

CREATE OR ALTER SCHEMA raw;

-- Schema level objects
CREATE OR REPLACE FILE FORMAT bronze.json_format TYPE = 'json';
CREATE OR ALTER STAGE raw.raw;


-- Copy file from GitHub to internal stage
copy files into @raw.raw from @git.public.git_repo/branches/main/data/airport_list.json;
