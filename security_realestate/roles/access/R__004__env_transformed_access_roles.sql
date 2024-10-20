-- ------------------------------------------------------------------------------------------------------------------------------
-- Create 'transformed' schema within <environment>_db database.
-- This schema is used for intermediate results and base tables.
-- ------------------------------------------------------------------------------------------------------------------------------
USE ROLE {{ defaultRole }};
USE ROLE {{ defaultRole }};
USE DATABASE {{ environment }}_db;
CREATE SCHEMA IF NOT EXISTS transformed;

-- Create access roles for the schema
USE ROLE USERADMIN;
CREATE ROLE IF NOT EXISTS  {{ environment }}_transformed_w;
CREATE ROLE IF NOT EXISTS  {{ environment }}_transformed_r;
CREATE ROLE IF NOT EXISTS  {{ environment }}_transformed_ddl;
CREATE ROLE IF NOT EXISTS  {{ environment }}_transformed_owner;

USE ROLE {{ defaultRole }};

-- Grant role to global_owner
GRANT ROLE {{ environment }}_transformed_owner TO ROLE global_owner;

-- Reader role
GRANT USAGE ON DATABASE {{ environment }}_db TO ROLE {{ environment }}_transformed_r;
GRANT USAGE ON SCHEMA {{ environment }}_db.transformed TO ROLE {{ environment }}_transformed_r;
GRANT SELECT ON ALL TABLES IN SCHEMA {{ environment }}_db.transformed TO ROLE {{ environment }}_transformed_r;
GRANT SELECT ON ALL EXTERNAL TABLES IN SCHEMA {{ environment }}_db.transformed TO ROLE {{ environment }}_transformed_r;
GRANT SELECT ON ALL VIEWS IN SCHEMA {{ environment }}_db.transformed TO ROLE {{ environment }}_transformed_r;
GRANT SELECT ON ALL MATERIALIZED VIEWS IN SCHEMA {{ environment }}_db.transformed TO ROLE {{ environment }}_transformed_r;
GRANT SELECT ON FUTURE TABLES IN SCHEMA {{ environment }}_db.transformed TO ROLE {{ environment }}_transformed_r;
GRANT SELECT ON FUTURE EXTERNAL TABLES IN SCHEMA {{ environment }}_db.transformed TO ROLE {{ environment }}_transformed_r;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA {{ environment }}_db.transformed TO ROLE {{ environment }}_transformed_r;
GRANT SELECT ON FUTURE MATERIALIZED VIEWS IN SCHEMA {{ environment }}_db.transformed TO ROLE {{ environment }}_transformed_r;
GRANT USAGE ON ALL FUNCTIONS IN SCHEMA {{ environment }}_db.transformed TO ROLE {{ environment }}_transformed_r;
GRANT USAGE ON FUTURE FUNCTIONS IN SCHEMA {{ environment }}_db.transformed TO ROLE {{ environment }}_transformed_r;
GRANT SELECT ON ALL STREAMS IN SCHEMA {{ environment }}_db.transformed TO ROLE {{ environment }}_transformed_r;
GRANT SELECT ON FUTURE STREAMS IN SCHEMA {{ environment }}_db.transformed TO ROLE {{ environment }}_transformed_r;

-- Writer role
GRANT USAGE ON DATABASE {{ environment }}_db TO ROLE {{ environment }}_transformed_w;
GRANT USAGE ON SCHEMA {{ environment }}_db.transformed TO ROLE {{ environment }}_transformed_w;
GRANT INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA {{ environment }}_db.transformed TO ROLE {{ environment }}_transformed_w;
GRANT INSERT, UPDATE, DELETE, TRUNCATE ON FUTURE TABLES IN SCHEMA {{ environment }}_db.transformed TO ROLE {{ environment }}_transformed_w;
GRANT USAGE ON ALL PROCEDURES IN SCHEMA {{ environment }}_db.transformed TO ROLE {{ environment }}_transformed_w;
GRANT USAGE ON FUTURE PROCEDURES IN SCHEMA {{ environment }}_db.transformed TO ROLE {{ environment }}_transformed_w;
GRANT USAGE ON ALL FUNCTIONS IN SCHEMA {{ environment }}_db.transformed TO ROLE {{ environment }}_transformed_w;
GRANT USAGE ON FUTURE FUNCTIONS IN SCHEMA {{ environment }}_db.transformed TO ROLE {{ environment }}_transformed_w;

-- DDL role
GRANT USAGE ON DATABASE {{ environment }}_db TO ROLE {{ environment }}_transformed_ddl;
GRANT USAGE ON SCHEMA {{ environment }}_db.transformed TO ROLE {{ environment }}_transformed_ddl;
GRANT CREATE TABLE ON SCHEMA {{ environment }}_db.transformed TO ROLE {{ environment }}_transformed_ddl;
GRANT CREATE VIEW ON SCHEMA {{ environment }}_db.transformed TO ROLE {{ environment }}_transformed_ddl;
GRANT CREATE MATERIALIZED VIEW ON SCHEMA {{ environment }}_db.transformed TO ROLE {{ environment }}_transformed_ddl;
GRANT CREATE STREAM ON SCHEMA {{ environment }}_db.transformed TO ROLE {{ environment }}_transformed_ddl;
GRANT CREATE TASK ON SCHEMA {{ environment }}_db.transformed TO ROLE {{ environment }}_transformed_ddl;
GRANT CREATE FUNCTION ON SCHEMA {{ environment }}_db.transformed TO ROLE {{ environment }}_transformed_ddl;
GRANT CREATE PROCEDURE ON SCHEMA {{ environment }}_db.transformed TO ROLE {{ environment }}_transformed_ddl;
GRANT CREATE SEQUENCE ON SCHEMA {{ environment }}_db.transformed TO ROLE {{ environment }}_transformed_ddl;

-- Owner role
GRANT OWNERSHIP ON SCHEMA {{ environment }}_db.transformed TO ROLE {{ environment }}_transformed_owner COPY CURRENT GRANTS;
GRANT OWNERSHIP ON ALL TABLES IN SCHEMA {{ environment }}_db.transformed TO ROLE {{ environment }}_transformed_owner COPY CURRENT GRANTS;
GRANT OWNERSHIP ON ALL EXTERNAL TABLES IN SCHEMA {{ environment }}_db.transformed TO ROLE {{ environment }}_transformed_owner COPY CURRENT GRANTS;
GRANT OWNERSHIP ON ALL VIEWS IN SCHEMA {{ environment }}_db.transformed TO ROLE {{ environment }}_transformed_owner COPY CURRENT GRANTS;
GRANT OWNERSHIP ON ALL MATERIALIZED VIEWS IN SCHEMA {{ environment }}_db.transformed TO ROLE {{ environment }}_transformed_owner COPY CURRENT GRANTS;
GRANT OWNERSHIP ON ALL STREAMS IN SCHEMA {{ environment }}_db.transformed TO ROLE {{ environment }}_transformed_owner COPY CURRENT GRANTS;
GRANT OWNERSHIP ON ALL PROCEDURES IN SCHEMA {{ environment }}_db.transformed TO ROLE {{ environment }}_transformed_owner COPY CURRENT GRANTS;
GRANT OWNERSHIP ON ALL FUNCTIONS IN SCHEMA {{ environment }}_db.transformed TO ROLE {{ environment }}_transformed_owner COPY CURRENT GRANTS;
GRANT OWNERSHIP ON ALL SEQUENCES IN SCHEMA {{ environment }}_db.transformed TO ROLE {{ environment }}_transformed_owner COPY CURRENT GRANTS;
GRANT OWNERSHIP ON ALL TASKS IN SCHEMA {{ environment }}_db.transformed TO ROLE {{ environment }}_transformed_owner COPY CURRENT GRANTS;

-- Before granting ownership on future objects, existing future grants in schema must be removed
SHOW FUTURE GRANTS IN SCHEMA transformed;

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
    cmd := 'REVOKE OWNERSHIP ON FUTURE ' || record.object_type || 'S IN SCHEMA {{ environment }}_db.transformed FROM ROLE ' || record.grantee_name;
    EXECUTE IMMEDIATE cmd;
  END FOR;
  RETURN 'DONE';
END;
$$;

GRANT OWNERSHIP ON FUTURE TABLES IN SCHEMA {{ environment }}_db.transformed TO ROLE {{ environment }}_transformed_owner;
GRANT OWNERSHIP ON FUTURE EXTERNAL TABLES IN SCHEMA {{ environment }}_db.transformed TO ROLE {{ environment }}_transformed_owner;
GRANT OWNERSHIP ON FUTURE VIEWS IN SCHEMA {{ environment }}_db.transformed TO ROLE {{ environment }}_transformed_owner;
GRANT OWNERSHIP ON FUTURE MATERIALIZED VIEWS IN SCHEMA {{ environment }}_db.transformed TO ROLE {{ environment }}_transformed_owner;
GRANT OWNERSHIP ON FUTURE STREAMS IN SCHEMA {{ environment }}_db.transformed TO ROLE {{ environment }}_transformed_owner;
GRANT OWNERSHIP ON FUTURE PROCEDURES IN SCHEMA {{ environment }}_db.transformed TO ROLE {{ environment }}_transformed_owner;
GRANT OWNERSHIP ON FUTURE FUNCTIONS IN SCHEMA {{ environment }}_db.transformed TO ROLE {{ environment }}_transformed_owner;
GRANT OWNERSHIP ON FUTURE SEQUENCES IN SCHEMA {{ environment }}_db.transformed TO ROLE {{ environment }}_transformed_owner;
GRANT OWNERSHIP ON FUTURE TASKS IN SCHEMA {{ environment }}_db.transformed TO ROLE {{ environment }}_transformed_owner;

-- All tasks in the schema are suspended automatically after ownership transfer.
-- Identify all root tasks and use command that also recursively resumes all dependent tasks.
SHOW TASKS IN {{ environment }}_db.transformed;
EXECUTE IMMEDIATE $$
DECLARE
  c1 CURSOR FOR 
    SELECT "database_name" AS database_name, "schema_name" AS schema_name,"name" AS name
    FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
    WHERE ARRAY_SIZE("predecessors") = 0 AND "state" = 'suspended';
  cmd STRING;
BEGIN
  FOR record IN c1 DO
    cmd := 'SELECT SYSTEM$TASK_DEPENDENTS_ENABLE(''' || record.database_name || '.' || record.schema_name || '.' || record.name || ''')';
    EXECUTE IMMEDIATE cmd;
  END FOR;
  RETURN 'DONE';
END;
$$;

-- To execute tasks task_admin role, which has EXECUTE MANAGED TASK privilege, must be granted to owner role
GRANT ROLE task_admin TO ROLE {{ environment }}_transformed_owner;

-- This is necessary in order to create views using objects from raw and utils schemas
GRANT ROLE {{ environment }}_raw_r TO ROLE {{ environment }}_transformed_owner;
GRANT ROLE {{ environment }}_utils_r TO ROLE {{ environment }}_transformed_owner;

-- DDL role has to have owner role in order to modify existing objects
GRANT ROLE {{ environment }}_transformed_owner TO ROLE {{ environment }}_transformed_ddl;

-- Grant read and write roles on schema to task_admin role (managed tasks are executed under this role)
-- This role is created by the account setup script
GRANT ROLE {{ environment }}_transformed_r TO ROLE task_admin;
GRANT ROLE {{ environment }}_transformed_w TO ROLE task_admin;
