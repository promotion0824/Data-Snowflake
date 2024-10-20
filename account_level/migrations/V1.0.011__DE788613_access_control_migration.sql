-- ------------------------------------------------------------------------------------------------------------------------------
-- Create role global_owner and grant Ownership of current objects to this role before it can be used to run the deployment pipeline
-- ------------------------------------------------------------------------------------------------------------------------------
USE ROLE USERADMIN;
CREATE ROLE IF NOT EXISTS global_owner;
GRANT ROLE global_owner TO ROLE deployment_pipeline;
GRANT ROLE global_owner TO ROLE SYSADMIN;

USE ROLE SYSADMIN;

-- Databases
GRANT OWNERSHIP ON DATABASE _{{ customerName }} TO ROLE global_owner COPY CURRENT GRANTS; 

-- Grant ownership of all objects to global_owner role first
-- Dev
GRANT OWNERSHIP ON DATABASE dev_db TO ROLE global_owner COPY CURRENT GRANTS; 
GRANT OWNERSHIP ON ALL SCHEMAS IN DATABASE dev_db TO ROLE global_owner COPY CURRENT GRANTS;
GRANT OWNERSHIP ON ALL TABLES IN DATABASE dev_db TO ROLE global_owner COPY CURRENT GRANTS; 
GRANT OWNERSHIP ON ALL VIEWS IN DATABASE dev_db TO ROLE global_owner COPY CURRENT GRANTS;
GRANT OWNERSHIP ON ALL TASKS IN DATABASE dev_db TO ROLE global_owner COPY CURRENT GRANTS;
GRANT OWNERSHIP ON ALL FUNCTIONS IN DATABASE dev_db TO ROLE global_owner COPY CURRENT GRANTS;
GRANT OWNERSHIP ON ALL PROCEDURES IN DATABASE dev_db TO ROLE global_owner COPY CURRENT GRANTS;
GRANT OWNERSHIP ON ALL STAGES IN DATABASE dev_db TO ROLE global_owner COPY CURRENT GRANTS;

-- -- UAT
GRANT OWNERSHIP ON DATABASE uat_db TO ROLE global_owner COPY CURRENT GRANTS; 
GRANT OWNERSHIP ON ALL SCHEMAS IN DATABASE uat_db TO ROLE global_owner COPY CURRENT GRANTS;
GRANT OWNERSHIP ON ALL TABLES IN DATABASE uat_db TO ROLE global_owner COPY CURRENT GRANTS; 
GRANT OWNERSHIP ON ALL VIEWS IN DATABASE uat_db TO ROLE global_owner COPY CURRENT GRANTS;
GRANT OWNERSHIP ON ALL TASKS IN DATABASE uat_db TO ROLE global_owner COPY CURRENT GRANTS;
GRANT OWNERSHIP ON ALL FUNCTIONS IN DATABASE uat_db TO ROLE global_owner COPY CURRENT GRANTS;
GRANT OWNERSHIP ON ALL PROCEDURES IN DATABASE uat_db TO ROLE global_owner COPY CURRENT GRANTS;
GRANT OWNERSHIP ON ALL STAGES IN DATABASE uat_db TO ROLE global_owner COPY CURRENT GRANTS;

-- -- PRD
GRANT OWNERSHIP ON DATABASE prd_db TO ROLE global_owner COPY CURRENT GRANTS; 
GRANT OWNERSHIP ON ALL SCHEMAS IN DATABASE prd_db TO ROLE global_owner COPY CURRENT GRANTS;
GRANT OWNERSHIP ON ALL TABLES IN DATABASE prd_db TO ROLE global_owner COPY CURRENT GRANTS; 
GRANT OWNERSHIP ON ALL VIEWS IN DATABASE prd_db TO ROLE global_owner COPY CURRENT GRANTS;
GRANT OWNERSHIP ON ALL TASKS IN DATABASE prd_db TO ROLE global_owner COPY CURRENT GRANTS;
GRANT OWNERSHIP ON ALL FUNCTIONS IN DATABASE prd_db TO ROLE global_owner COPY CURRENT GRANTS;
GRANT OWNERSHIP ON ALL PROCEDURES IN DATABASE prd_db TO ROLE global_owner COPY CURRENT GRANTS;
GRANT OWNERSHIP ON ALL STAGES IN DATABASE prd_db TO ROLE global_owner COPY CURRENT GRANTS;

-- Util_db
GRANT OWNERSHIP ON DATABASE util_db TO ROLE global_owner COPY CURRENT GRANTS; 
GRANT OWNERSHIP ON ALL SCHEMAS IN DATABASE util_db TO ROLE global_owner COPY CURRENT GRANTS;
GRANT OWNERSHIP ON ALL TABLES IN DATABASE util_db TO ROLE global_owner COPY CURRENT GRANTS; 
GRANT OWNERSHIP ON ALL VIEWS IN DATABASE util_db TO ROLE global_owner COPY CURRENT GRANTS;
GRANT OWNERSHIP ON ALL TASKS IN DATABASE util_db TO ROLE global_owner COPY CURRENT GRANTS;
GRANT OWNERSHIP ON ALL FUNCTIONS IN DATABASE util_db TO ROLE global_owner COPY CURRENT GRANTS;
GRANT OWNERSHIP ON ALL PROCEDURES IN DATABASE util_db TO ROLE global_owner COPY CURRENT GRANTS;

-- analytics_db
GRANT OWNERSHIP ON DATABASE analytics_db TO ROLE global_owner COPY CURRENT GRANTS; 

GRANT OWNERSHIP ON ALL SCHEMAS IN DATABASE analytics_db TO ROLE global_owner COPY CURRENT GRANTS;

-- monitoring_db
GRANT OWNERSHIP ON DATABASE monitoring_db TO ROLE global_owner COPY CURRENT GRANTS; 

GRANT OWNERSHIP ON ALL SCHEMAS IN DATABASE monitoring_db TO ROLE global_owner COPY CURRENT GRANTS;

-- ------------------------------------------------------------------------------------
-- Transfer ownership of existing integrations
-- Grant usage to integrations_user
-- ------------------------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;

SHOW INTEGRATIONS;

EXECUTE IMMEDIATE $$
DECLARE
  c1 CURSOR FOR 
    SELECT "name" AS name
    FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
    WHERE "category" IN ('STORAGE', 'NOTIFICATION'); 
  name STRING;
  cmd STRING;
BEGIN
  FOR record IN c1 DO
    cmd := 'GRANT OWNERSHIP ON INTEGRATION ' || record.name || ' TO ROLE global_owner REVOKE CURRENT GRANTS';
    EXECUTE IMMEDIATE cmd;
    cmd := 'GRANT USAGE ON INTEGRATION ' || record.name || ' TO ROLE integrations_user';
    EXECUTE IMMEDIATE cmd;    
  END FOR;
  RETURN 'DONE';
END;
$$;

USE ROLE SYSADMIN;

-- ------------------------------------------------------------------------------------
-- Transfer ownership of existing pipes
-- This is not possible to do in bulk and pipes must be paused
-- ------------------------------------------------------------------------------------
-- Dev
USE ROLE SYSADMIN;
ALTER PIPE dev_db.raw.ingest_raw_from_ext_stage_pp SET PIPE_EXECUTION_PAUSED=true;
ALTER PIPE dev_db.raw.ingest_telemetry_pp SET PIPE_EXECUTION_PAUSED=true;

GRANT OWNERSHIP ON PIPE dev_db.raw.ingest_raw_from_ext_stage_pp TO ROLE deployment_pipeline REVOKE CURRENT GRANTS;
GRANT MONITOR ON PIPE dev_db.raw.ingest_raw_from_ext_stage_pp TO ROLE monitoring_pipeline_reader;

GRANT OWNERSHIP ON PIPE dev_db.raw.ingest_telemetry_pp TO ROLE deployment_pipeline REVOKE CURRENT GRANTS;
GRANT MONITOR ON PIPE dev_db.raw.ingest_telemetry_pp TO ROLE monitoring_pipeline_reader;

USE ROLE deployment_pipeline;
USE SCHEMA dev_db.raw;
SELECT SYSTEM$PIPE_FORCE_RESUME('INGEST_RAW_FROM_EXT_STAGE_PP');
SELECT SYSTEM$PIPE_FORCE_RESUME('INGEST_TELEMETRY_PP');

ALTER PIPE dev_db.raw.ingest_raw_from_ext_stage_pp SET PIPE_EXECUTION_PAUSED=false;
ALTER PIPE dev_db.raw.ingest_telemetry_pp SET PIPE_EXECUTION_PAUSED=false;

-- UAT
USE ROLE SYSADMIN;

ALTER PIPE uat_db.raw.ingest_raw_from_ext_stage_pp SET PIPE_EXECUTION_PAUSED=true;
ALTER PIPE uat_db.raw.ingest_telemetry_pp SET PIPE_EXECUTION_PAUSED=true;

GRANT OWNERSHIP ON PIPE uat_db.raw.ingest_raw_from_ext_stage_pp TO ROLE deployment_pipeline REVOKE CURRENT GRANTS;
GRANT MONITOR ON PIPE uat_db.raw.ingest_raw_from_ext_stage_pp TO ROLE monitoring_pipeline_reader;

GRANT OWNERSHIP ON PIPE uat_db.raw.ingest_telemetry_pp TO ROLE deployment_pipeline REVOKE CURRENT GRANTS;
GRANT MONITOR ON PIPE uat_db.raw.ingest_telemetry_pp TO ROLE monitoring_pipeline_reader;

USE ROLE deployment_pipeline;
USE SCHEMA uat_db.raw;
SELECT SYSTEM$PIPE_FORCE_RESUME('INGEST_RAW_FROM_EXT_STAGE_PP');
SELECT SYSTEM$PIPE_FORCE_RESUME('INGEST_TELEMETRY_PP');

ALTER PIPE uat_db.raw.ingest_raw_from_ext_stage_pp SET PIPE_EXECUTION_PAUSED=false;
ALTER PIPE uat_db.raw.ingest_telemetry_pp SET PIPE_EXECUTION_PAUSED=false;

-- Prd
USE ROLE SYSADMIN;
ALTER PIPE prd_db.raw.ingest_raw_from_ext_stage_pp SET PIPE_EXECUTION_PAUSED=true;
ALTER PIPE prd_db.raw.ingest_telemetry_pp SET PIPE_EXECUTION_PAUSED=true;

GRANT OWNERSHIP ON PIPE prd_db.raw.ingest_raw_from_ext_stage_pp TO ROLE deployment_pipeline REVOKE CURRENT GRANTS;
GRANT MONITOR ON PIPE prd_db.raw.ingest_raw_from_ext_stage_pp TO ROLE monitoring_pipeline_reader;

GRANT OWNERSHIP ON PIPE prd_db.raw.ingest_telemetry_pp TO ROLE deployment_pipeline REVOKE CURRENT GRANTS;
GRANT MONITOR ON PIPE prd_db.raw.ingest_telemetry_pp TO ROLE monitoring_pipeline_reader;

USE ROLE deployment_pipeline;
USE SCHEMA prd_db.raw;
SELECT SYSTEM$PIPE_FORCE_RESUME('INGEST_RAW_FROM_EXT_STAGE_PP');
SELECT SYSTEM$PIPE_FORCE_RESUME('INGEST_TELEMETRY_PP');

ALTER PIPE prd_db.raw.ingest_raw_from_ext_stage_pp SET PIPE_EXECUTION_PAUSED=false;
ALTER PIPE prd_db.raw.ingest_telemetry_pp SET PIPE_EXECUTION_PAUSED=false;

USE ROLE SYSADMIN;
GRANT USAGE ON WAREHOUSE compute_wh TO ROLE SECURITYADMIN;
USE ROLE SECURITYADMIN;
USE WAREHOUSE compute_wh;
SHOW USERS;
EXECUTE IMMEDIATE $$
DECLARE
  c1 CURSOR FOR 
    SELECT "name" AS name
    FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
    WHERE "name" NOT IN ('DATAENGINEERINGSERVICE', 'SNOWFLAKE');
  name STRING;
  cmd STRING;
BEGIN
  FOR record IN c1 DO
    cmd := 'GRANT OWNERSHIP ON USER ' || record.name || ' TO ROLE USERADMIN COPY CURRENT GRANTS';
    EXECUTE IMMEDIATE cmd;
  END FOR;
  RETURN 'DONE';
END;
$$;

-- Transfer ownership of all roles to USERADMIN
SHOW ROLES;
EXECUTE IMMEDIATE $$
DECLARE
  c1 CURSOR FOR 
    SELECT "name" AS name
    FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
    WHERE "name" NOT IN ('PUBLIC', 'ACCOUNTADMIN', 'SECURITYADMIN', 'SYSADMIN', 'USERADMIN');
  name STRING;
  cmd STRING;
BEGIN
  FOR record IN c1 DO
    cmd := 'GRANT OWNERSHIP ON ROLE ' || record.name || ' TO ROLE USERADMIN COPY CURRENT GRANTS';
    EXECUTE IMMEDIATE cmd;
  END FOR;
  RETURN 'DONE';
END;
$$;

-- Transfer ownership of all file formats to deployment_pipeline
USE ROLE SYSADMIN;

USE WAREHOUSE deployment_pipeline_wh;
SHOW FILE FORMATS IN ACCOUNT;
EXECUTE IMMEDIATE $$
DECLARE
  c1 CURSOR FOR 
    SELECT "database_name" AS database_name, "schema_name" AS schema_name,"name" AS name
    FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
  name STRING;
  cmd STRING;
BEGIN
  FOR record IN c1 DO
    cmd := 'GRANT OWNERSHIP ON FILE FORMAT  ' || record.database_name || '.' || record.schema_name || '.' || record.name || ' TO ROLE deployment_pipeline COPY CURRENT GRANTS';
    EXECUTE IMMEDIATE cmd;
  END FOR;
  RETURN 'DONE';
END;
$$;

USE ROLE SYSADMIN;
REVOKE USAGE ON WAREHOUSE compute_wh FROM ROLE SECURITYADMIN;

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE compute_wh;
SHOW WAREHOUSES;
EXECUTE IMMEDIATE $$
DECLARE
  c1 CURSOR FOR 
    SELECT "name" AS name
    FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
  name STRING;
  cmd STRING;
BEGIN
  FOR record IN c1 DO
    cmd := 'GRANT OWNERSHIP ON WAREHOUSE  ' || record.name || ' TO ROLE deployment_pipeline COPY CURRENT GRANTS';
    EXECUTE IMMEDIATE cmd;
  END FOR;
  RETURN 'DONE';
END;
$$;

USE ROLE SYSADMIN;

-- Drop tasks that are migrated to transformed schema
-- This needs to be done prior to persmissisons deploymentDROP TASK IF EXISTS dev_db.raw.merge_directory_core_sites_stream_tk;
DROP TASK IF EXISTS dev_db.raw.merge_twins_relationships_stream_tk;
DROP TASK IF EXISTS dev_db.raw.merge_directory_core_sites_stream_tk;
DROP TASK IF EXISTS dev_db.raw.create_table_transformed_hvac_equipment_tk; 
DROP TASK IF EXISTS dev_db.raw.create_table_transformed_occupancy_tk;
DROP TASK IF EXISTS dev_db.raw.create_table_transformed_sites_tk;
DROP TASK IF EXISTS dev_db.raw.create_table_transformed_hvac_adjusted_capabilities_tk; 
DROP TASK IF EXISTS dev_db.raw.create_table_transformed_capabilities_hvac_occupancy_tk;

DROP TASK IF EXISTS uat_db.raw.merge_directory_core_sites_stream_tk;
DROP TASK IF EXISTS uat_db.raw.merge_twins_relationships_stream_tk;
DROP TASK IF EXISTS uat_db.raw.merge_directory_core_sites_stream_tk;
DROP TASK IF EXISTS uat_db.raw.create_table_transformed_hvac_equipment_tk; 
DROP TASK IF EXISTS uat_db.raw.create_table_transformed_occupancy_tk;
DROP TASK IF EXISTS uat_db.raw.create_table_transformed_sites_tk;
DROP TASK IF EXISTS uat_db.raw.create_table_transformed_hvac_adjusted_capabilities_tk; 
DROP TASK IF EXISTS uat_db.raw.create_table_transformed_capabilities_hvac_occupancy_tk;

DROP TASK IF EXISTS prd_db.raw.merge_directory_core_sites_stream_tk;
DROP TASK IF EXISTS prd_db.raw.merge_twins_relationships_stream_tk;
DROP TASK IF EXISTS prd_db.raw.merge_directory_core_sites_stream_tk;
DROP TASK IF EXISTS prd_db.raw.create_table_transformed_hvac_equipment_tk; 
DROP TASK IF EXISTS prd_db.raw.create_table_transformed_occupancy_tk;
DROP TASK IF EXISTS prd_db.raw.create_table_transformed_sites_tk;
DROP TASK IF EXISTS prd_db.raw.create_table_transformed_hvac_adjusted_capabilities_tk; 
DROP TASK IF EXISTS prd_db.raw.create_table_transformed_capabilities_hvac_occupancy_tk;
