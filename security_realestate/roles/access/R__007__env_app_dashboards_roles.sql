-- -- ------------------------------------------------------------------------------------------------------------------------------
-- -- Create 'app_dashboards' schema within <environment>_db database.
-- -- This schema is used for Enterprise Dashboards
-- -- ------------------------------------------------------------------------------------------------------------------------------
USE ROLE {{ defaultRole }};
USE DATABASE {{ environment }}_db;
CREATE SCHEMA IF NOT EXISTS app_dashboards;

-- Create access roles for the schema
USE ROLE USERADMIN;
CREATE ROLE IF NOT EXISTS  {{ environment }}_app_dashboards_r;
CREATE ROLE IF NOT EXISTS  {{ environment }}_app_dashboards_ddl;
CREATE ROLE IF NOT EXISTS  {{ environment }}_app_dashboards_owner;

USE ROLE {{ defaultRole }};

-- Grant role to global_owner
GRANT ROLE {{ environment }}_app_dashboards_owner TO ROLE global_owner;

-- Reader role
GRANT USAGE ON DATABASE {{ environment }}_db TO ROLE {{ environment }}_app_dashboards_r;
GRANT USAGE ON SCHEMA {{ environment }}_db.app_dashboards TO ROLE {{ environment }}_app_dashboards_r;
GRANT USAGE ON ALL FUNCTIONS IN SCHEMA {{ environment }}_db.app_dashboards TO ROLE {{ environment }}_app_dashboards_r;
GRANT USAGE ON FUTURE FUNCTIONS IN SCHEMA {{ environment }}_db.app_dashboards TO ROLE {{ environment }}_app_dashboards_r;

-- DDL role
GRANT USAGE ON DATABASE {{ environment }}_db TO ROLE {{ environment }}_app_dashboards_ddl;
GRANT USAGE ON SCHEMA {{ environment }}_db.app_dashboards TO ROLE {{ environment }}_app_dashboards_ddl;
GRANT CREATE FUNCTION ON SCHEMA {{ environment }}_db.app_dashboards TO ROLE {{ environment }}_app_dashboards_ddl;

-- Owner role
GRANT OWNERSHIP ON SCHEMA {{ environment }}_db.app_dashboards TO ROLE {{ environment }}_app_dashboards_owner COPY CURRENT GRANTS;
GRANT OWNERSHIP ON ALL FUNCTIONS IN SCHEMA {{ environment }}_db.app_dashboards TO ROLE {{ environment }}_app_dashboards_owner COPY CURRENT GRANTS;

-- Before granting ownership on future objects, existing future grants in schema must be removed
SHOW FUTURE GRANTS IN SCHEMA app_dashboards;

EXECUTE IMMEDIATE $$
DECLARE
  c1 CURSOR FOR 
    SELECT REPLACE("grant_on", '_', ' ') AS object_type, "grantee_name" AS grantee_name 
    FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
    WHERE "privilege" = 'OWNERSHIP';  
  object_type STRING;
  grantee_name STRING;
  cmd STRING;
BEGIN
  FOR record IN c1 DO
    cmd := 'REVOKE OWNERSHIP ON FUTURE ' || record.object_type || 'S IN SCHEMA {{ environment }}_db.app_dashboards FROM ROLE ' || record.grantee_name;
    EXECUTE IMMEDIATE cmd;
  END FOR;
  RETURN 'DONE';
END;
$$;

GRANT OWNERSHIP ON FUTURE FUNCTIONS IN SCHEMA {{ environment }}_db.app_dashboards TO ROLE {{ environment }}_app_dashboards_owner;

-- This is necessary in order to use objects from transformed schema
GRANT ROLE {{ environment }}_transformed_r TO ROLE {{ environment }}_app_dashboards_owner;
GRANT ROLE {{ environment }}_published_r TO ROLE {{ environment }}_app_dashboards_owner;

-- DDL role has to have owner role in order to modify existing objects
GRANT ROLE {{ environment }}_app_dashboards_owner TO ROLE {{ environment }}_app_dashboards_ddl;
