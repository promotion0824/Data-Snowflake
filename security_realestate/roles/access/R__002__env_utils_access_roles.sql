-- -- ------------------------------------------------------------------------------------------------------------------------------
-- -- Create 'utils' schema within <environment>_db database.
-- -- This schema is used for shared utilities such as lookup tables, stored procedures and functions.
-- -- ------------------------------------------------------------------------------------------------------------------------------

USE ROLE {{ defaultRole }};
USE DATABASE {{ environment }}_db;
CREATE SCHEMA IF NOT EXISTS utils;

-- Create access roles for the schema
USE ROLE USERADMIN;
CREATE ROLE IF NOT EXISTS  {{ environment }}_utils_w;
CREATE ROLE IF NOT EXISTS  {{ environment }}_utils_r;
CREATE ROLE IF NOT EXISTS  {{ environment }}_utils_ddl;
CREATE ROLE IF NOT EXISTS  {{ environment }}_utils_owner;

USE ROLE {{ defaultRole }};

-- Grant role to global_owner
GRANT ROLE {{ environment }}_utils_owner TO ROLE global_owner;

-- Reader role
GRANT USAGE ON DATABASE {{ environment }}_db TO ROLE {{ environment }}_utils_r;
GRANT USAGE ON SCHEMA {{ environment }}_db.utils TO ROLE {{ environment }}_utils_r;
GRANT SELECT ON ALL TABLES IN SCHEMA {{ environment }}_db.utils TO ROLE {{ environment }}_utils_r;
GRANT SELECT ON ALL VIEWS IN SCHEMA {{ environment }}_db.utils TO ROLE {{ environment }}_utils_r;
GRANT SELECT ON FUTURE TABLES IN SCHEMA {{ environment }}_db.utils TO ROLE {{ environment }}_utils_r;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA {{ environment }}_db.utils TO ROLE {{ environment }}_utils_r;
GRANT USAGE ON ALL FUNCTIONS IN SCHEMA {{ environment }}_db.utils TO ROLE {{ environment }}_utils_r;
GRANT USAGE ON FUTURE FUNCTIONS IN SCHEMA {{ environment }}_db.utils TO ROLE {{ environment }}_utils_r;
GRANT USAGE ON ALL FILE FORMATS IN SCHEMA {{ environment }}_db.utils TO ROLE {{ environment }}_utils_r;
GRANT USAGE ON FUTURE FILE FORMATS IN SCHEMA {{ environment }}_db.utils TO ROLE {{ environment }}_utils_r;

-- Writer role
GRANT USAGE ON DATABASE {{ environment }}_db TO ROLE {{ environment }}_utils_w;
GRANT USAGE ON SCHEMA {{ environment }}_db.utils TO ROLE {{ environment }}_utils_w;
GRANT INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA {{ environment }}_db.utils TO ROLE {{ environment }}_utils_w;
GRANT INSERT, UPDATE, DELETE, TRUNCATE ON FUTURE TABLES IN SCHEMA {{ environment }}_db.utils TO ROLE {{ environment }}_utils_w;
GRANT USAGE ON ALL PROCEDURES IN SCHEMA {{ environment }}_db.utils TO ROLE {{ environment }}_utils_w;
GRANT USAGE ON FUTURE PROCEDURES IN SCHEMA {{ environment }}_db.utils TO ROLE {{ environment }}_utils_w;
GRANT USAGE ON ALL FUNCTIONS IN SCHEMA {{ environment }}_db.utils TO ROLE {{ environment }}_utils_w;
GRANT USAGE ON FUTURE FUNCTIONS IN SCHEMA {{ environment }}_db.utils TO ROLE {{ environment }}_utils_w;

-- DDL role
GRANT USAGE ON DATABASE {{ environment }}_db TO ROLE {{ environment }}_utils_ddl;
GRANT USAGE ON SCHEMA {{ environment }}_db.utils TO ROLE {{ environment }}_utils_ddl;
GRANT CREATE TABLE ON SCHEMA {{ environment }}_db.utils TO ROLE {{ environment }}_utils_ddl;
GRANT CREATE VIEW ON SCHEMA {{ environment }}_db.utils TO ROLE {{ environment }}_utils_ddl;
GRANT CREATE FILE FORMAT ON SCHEMA {{ environment }}_db.utils TO ROLE {{ environment }}_utils_ddl;
GRANT CREATE FUNCTION ON SCHEMA {{ environment }}_db.utils TO ROLE {{ environment }}_utils_ddl;
GRANT CREATE PROCEDURE ON SCHEMA {{ environment }}_db.utils TO ROLE {{ environment }}_utils_ddl;
GRANT CREATE SEQUENCE ON SCHEMA {{ environment }}_db.utils TO ROLE {{ environment }}_utils_ddl;

-- Owner role
GRANT OWNERSHIP ON SCHEMA {{ environment }}_db.utils TO ROLE {{ environment }}_utils_owner COPY CURRENT GRANTS;
GRANT OWNERSHIP ON ALL TABLES IN SCHEMA {{ environment }}_db.utils TO ROLE {{ environment }}_utils_owner COPY CURRENT GRANTS;
GRANT OWNERSHIP ON ALL EXTERNAL TABLES IN SCHEMA {{ environment }}_db.utils TO ROLE {{ environment }}_utils_owner COPY CURRENT GRANTS;
GRANT OWNERSHIP ON ALL VIEWS IN SCHEMA {{ environment }}_db.utils TO ROLE {{ environment }}_utils_owner COPY CURRENT GRANTS;
GRANT OWNERSHIP ON ALL FILE FORMATS IN SCHEMA {{ environment }}_db.utils TO ROLE {{ environment }}_utils_owner COPY CURRENT GRANTS;
GRANT OWNERSHIP ON ALL PROCEDURES IN SCHEMA {{ environment }}_db.utils TO ROLE {{ environment }}_utils_owner COPY CURRENT GRANTS;
GRANT OWNERSHIP ON ALL FUNCTIONS IN SCHEMA {{ environment }}_db.utils TO ROLE {{ environment }}_utils_owner COPY CURRENT GRANTS;
GRANT OWNERSHIP ON ALL SEQUENCES IN SCHEMA {{ environment }}_db.utils TO ROLE {{ environment }}_utils_owner COPY CURRENT GRANTS;

-- Before granting ownership on future objects, existing future grants in schema must be removed
SHOW FUTURE GRANTS IN SCHEMA utils;

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
    cmd := 'REVOKE OWNERSHIP ON FUTURE ' || record.object_type || 'S IN SCHEMA {{ environment }}_db.utils FROM ROLE ' || record.grantee_name;
    EXECUTE IMMEDIATE cmd;
  END FOR;
  RETURN 'DONE';
END;
$$;

GRANT OWNERSHIP ON FUTURE TABLES IN SCHEMA {{ environment }}_db.utils TO ROLE {{ environment }}_utils_owner;
GRANT OWNERSHIP ON FUTURE EXTERNAL TABLES IN SCHEMA {{ environment }}_db.utils TO ROLE {{ environment }}_utils_owner;
GRANT OWNERSHIP ON FUTURE VIEWS IN SCHEMA {{ environment }}_db.utils TO ROLE {{ environment }}_utils_owner;
GRANT OWNERSHIP ON FUTURE FILE FORMATS IN SCHEMA {{ environment }}_db.utils TO ROLE {{ environment }}_utils_owner;
GRANT OWNERSHIP ON FUTURE PROCEDURES IN SCHEMA {{ environment }}_db.utils TO ROLE {{ environment }}_utils_owner;
GRANT OWNERSHIP ON FUTURE FUNCTIONS IN SCHEMA {{ environment }}_db.utils TO ROLE {{ environment }}_utils_owner;
GRANT OWNERSHIP ON FUTURE SEQUENCES IN SCHEMA {{ environment }}_db.utils TO ROLE {{ environment }}_utils_owner;

-- This is necessary in order to create views using objects from util_db and schemachange schema
GRANT ROLE util_public_r TO ROLE {{ environment }}_utils_owner;
GRANT ROLE util_schemachange_r TO ROLE {{ environment }}_utils_owner;
GRANT ROLE {{ environment }}_schemachange_r TO ROLE {{ environment }}_utils_owner;

-- DDL role has to have owner role in order to modify existing objects
GRANT ROLE {{ environment }}_utils_owner TO ROLE {{ environment }}_utils_ddl;
