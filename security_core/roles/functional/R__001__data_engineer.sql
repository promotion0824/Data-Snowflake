-- ------------------------------------------------------------------------------------------------------------------------------
-- Create 'data_engineer' functional role
-- ------------------------------------------------------------------------------------------------------------------------------

USE ROLE USERADMIN;

CREATE ROLE IF NOT EXISTS data_engineer;

USE ROLE {{ defaultRole }};

-- ------------------------------------------------------------------------------------------------------------------------------
-- Grant roles
-- ------------------------------------------------------------------------------------------------------------------------------
-- Grant execution monitor role
GRANT ROLE execution_monitor TO ROLE data_engineer;

-- Grant task_admin role
GRANT ROLE task_admin TO ROLE data_engineer;

-- Grant integration_user role
GRANT ROLE integrations_user TO ROLE data_engineer;

-- Grant database_creator role
GRANT ROLE database_creator TO ROLE data_engineer;

-- Snowflake database permissions
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE data_engineer; 

-- Usage on util_db database 
GRANT USAGE ON DATABASE util_db TO ROLE data_engineer;
GRANT USAGE ON SCHEMA util_db.schemachange TO ROLE data_engineer;

-- Read-only on all tables in schemachange schema
GRANT SELECT ON ALL TABLES IN SCHEMA util_db.schemachange TO ROLE data_engineer;

-- Usage on dummy _<customerAbbrv> database 
GRANT USAGE ON DATABASE _{{ customerAbbrv }} TO ROLE data_engineer;

USE ROLE {{ defaultRole }};