-- -- ------------------------------------------------------------------------------------------------------------------------------
-- -- Create 'schemachange' schema within <environment>_db database.
-- -- This schema is used for schemachange.
-- -- ------------------------------------------------------------------------------------------------------------------------------

USE ROLE {{ defaultRole }};
USE DATABASE {{ environment }}_db;
CREATE SCHEMA IF NOT EXISTS schemachange;

-- Create access roles for the schema

USE ROLE USERADMIN;

CREATE ROLE IF NOT EXISTS  {{ environment }}_schemachange_w;
CREATE ROLE IF NOT EXISTS  {{ environment }}_schemachange_r;
CREATE ROLE IF NOT EXISTS  {{ environment }}_schemachange_ddl;
CREATE ROLE IF NOT EXISTS  {{ environment }}_schemachange_owner;

USE ROLE {{ defaultRole }};

-- Grant role to global_owner
GRANT ROLE {{ environment }}_schemachange_owner TO ROLE global_owner;

-- Reader role
GRANT USAGE ON DATABASE {{ environment }}_db TO ROLE {{ environment }}_schemachange_r;
GRANT USAGE ON SCHEMA {{ environment }}_db.schemachange TO ROLE {{ environment }}_schemachange_r;
GRANT SELECT ON ALL TABLES IN SCHEMA {{ environment }}_db.schemachange TO ROLE {{ environment }}_schemachange_r;
GRANT SELECT ON ALL VIEWS IN SCHEMA {{ environment }}_db.schemachange TO ROLE {{ environment }}_schemachange_r;
GRANT SELECT ON FUTURE TABLES IN SCHEMA {{ environment }}_db.schemachange TO ROLE {{ environment }}_schemachange_r;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA {{ environment }}_db.schemachange TO ROLE {{ environment }}_schemachange_r;

-- Writer role
GRANT USAGE ON DATABASE {{ environment }}_db TO ROLE {{ environment }}_schemachange_w;
GRANT USAGE ON SCHEMA {{ environment }}_db.schemachange TO ROLE {{ environment }}_schemachange_w;
GRANT INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA {{ environment }}_db.schemachange TO ROLE {{ environment }}_schemachange_w;
GRANT INSERT, UPDATE, DELETE, TRUNCATE ON FUTURE TABLES IN SCHEMA {{ environment }}_db.schemachange TO ROLE {{ environment }}_schemachange_w;

-- DDL role
GRANT USAGE ON DATABASE {{ environment }}_db TO ROLE {{ environment }}_schemachange_ddl;
GRANT USAGE ON SCHEMA {{ environment }}_db.schemachange TO ROLE {{ environment }}_schemachange_ddl;
GRANT CREATE TABLE ON SCHEMA {{ environment }}_db.schemachange TO ROLE {{ environment }}_schemachange_ddl;
GRANT CREATE VIEW ON SCHEMA {{ environment }}_db.schemachange TO ROLE {{ environment }}_schemachange_ddl;

-- Owner role
GRANT OWNERSHIP ON SCHEMA {{ environment }}_db.schemachange TO ROLE {{ environment }}_schemachange_owner COPY CURRENT GRANTS;
GRANT OWNERSHIP ON ALL TABLES IN SCHEMA {{ environment }}_db.schemachange TO ROLE {{ environment }}_schemachange_owner COPY CURRENT GRANTS;
GRANT OWNERSHIP ON ALL VIEWS IN SCHEMA {{ environment }}_db.schemachange TO ROLE {{ environment }}_schemachange_owner COPY CURRENT GRANTS;

-- Before granting ownership on future objects, existing future grants in schema must be removed
SHOW FUTURE GRANTS IN SCHEMA schemachange;

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
    cmd := 'REVOKE OWNERSHIP ON FUTURE ' || record.object_type || 'S IN SCHEMA {{ environment }}_db.schemachange FROM ROLE ' || record.grantee_name;
    EXECUTE IMMEDIATE cmd;
  END FOR;
  RETURN 'DONE';
END;
$$;

GRANT OWNERSHIP ON FUTURE TABLES IN SCHEMA {{ environment }}_db.schemachange TO ROLE {{ environment }}_schemachange_owner;
GRANT OWNERSHIP ON FUTURE EXTERNAL TABLES IN SCHEMA {{ environment }}_db.schemachange TO ROLE {{ environment }}_schemachange_owner;
GRANT OWNERSHIP ON FUTURE VIEWS IN SCHEMA {{ environment }}_db.schemachange TO ROLE {{ environment }}_schemachange_owner;

-- DDL role has to have owner role in order to modify existing objects
GRANT ROLE {{ environment }}_schemachange_owner TO ROLE {{ environment }}_schemachange_ddl;
