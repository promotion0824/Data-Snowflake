-- ------------------------------------------------------------------------------------------------------------------------------
-- Create 'published' schema within <environment>_db database.
-- This schema is used for views ready for consumption by end users and BI tools.
-- ------------------------------------------------------------------------------------------------------------------------------
USE ROLE {{ defaultRole }};
USE DATABASE {{ environment }}_db;
CREATE SCHEMA IF NOT EXISTS published;

-- Create access roles for the schema
USE ROLE USERADMIN;
CREATE ROLE IF NOT EXISTS  {{ environment }}_published_w;
CREATE ROLE IF NOT EXISTS  {{ environment }}_published_r;
CREATE ROLE IF NOT EXISTS  {{ environment }}_published_ddl;
CREATE ROLE IF NOT EXISTS  {{ environment }}_published_owner;

USE ROLE {{ defaultRole }};

-- Grant role to global_owner
GRANT ROLE {{ environment }}_published_owner TO ROLE global_owner;

-- Reader role
GRANT USAGE ON DATABASE {{ environment }}_db TO ROLE {{ environment }}_published_r;
GRANT USAGE ON SCHEMA {{ environment }}_db.published TO ROLE {{ environment }}_published_r;
GRANT SELECT ON ALL VIEWS  IN SCHEMA {{ environment }}_db.published TO ROLE {{ environment }}_published_r;
GRANT SELECT ON FUTURE VIEWS  IN SCHEMA {{ environment }}_db.published TO ROLE {{ environment }}_published_r;

-- Writer role
GRANT USAGE ON DATABASE {{ environment }}_db TO ROLE {{ environment }}_published_w;
GRANT USAGE ON SCHEMA {{ environment }}_db.published TO ROLE {{ environment }}_published_w;

-- DDL role
GRANT USAGE ON DATABASE {{ environment }}_db TO ROLE {{ environment }}_published_ddl;
GRANT USAGE ON SCHEMA {{ environment }}_db.published TO ROLE {{ environment }}_published_ddl;
GRANT CREATE VIEW ON SCHEMA {{ environment }}_db.published TO ROLE {{ environment }}_published_ddl;

-- Owner role
GRANT OWNERSHIP ON SCHEMA {{ environment }}_db.published TO ROLE {{ environment }}_published_owner COPY CURRENT GRANTS;
GRANT OWNERSHIP ON ALL TABLES IN SCHEMA {{ environment }}_db.published TO ROLE {{ environment }}_published_owner COPY CURRENT GRANTS;
GRANT OWNERSHIP ON ALL EXTERNAL TABLES IN SCHEMA {{ environment }}_db.published TO ROLE {{ environment }}_published_owner COPY CURRENT GRANTS;
GRANT OWNERSHIP ON ALL VIEWS IN SCHEMA {{ environment }}_db.published TO ROLE {{ environment }}_published_owner COPY CURRENT GRANTS;
GRANT OWNERSHIP ON ALL MATERIALIZED VIEWS IN SCHEMA {{ environment }}_db.published TO ROLE {{ environment }}_published_owner COPY CURRENT GRANTS;
GRANT OWNERSHIP ON ALL PROCEDURES IN SCHEMA {{ environment }}_db.published TO ROLE {{ environment }}_published_owner COPY CURRENT GRANTS;
GRANT OWNERSHIP ON ALL FUNCTIONS IN SCHEMA {{ environment }}_db.published TO ROLE {{ environment }}_published_owner COPY CURRENT GRANTS;
-- Before granting ownership on future objects, existing future grants in schema must be removed
SHOW FUTURE GRANTS IN SCHEMA published;

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
    cmd := 'REVOKE OWNERSHIP ON FUTURE ' || record.object_type || 'S IN SCHEMA {{ environment }}_db.published FROM ROLE ' || record.grantee_name;
    EXECUTE IMMEDIATE cmd;
  END FOR;
  RETURN 'DONE';
END;
$$;

GRANT OWNERSHIP ON FUTURE TABLES IN SCHEMA {{ environment }}_db.published TO ROLE {{ environment }}_published_owner;
GRANT OWNERSHIP ON FUTURE EXTERNAL TABLES IN SCHEMA {{ environment }}_db.published TO ROLE {{ environment }}_published_owner;
GRANT OWNERSHIP ON FUTURE VIEWS IN SCHEMA {{ environment }}_db.published TO ROLE {{ environment }}_published_owner;
GRANT OWNERSHIP ON FUTURE MATERIALIZED VIEWS IN SCHEMA {{ environment }}_db.published TO ROLE {{ environment }}_published_owner;
GRANT OWNERSHIP ON FUTURE PROCEDURES IN SCHEMA {{ environment }}_db.published TO ROLE {{ environment }}_published_owner;
GRANT OWNERSHIP ON FUTURE FUNCTIONS IN SCHEMA {{ environment }}_db.published TO ROLE {{ environment }}_published_owner;

-- This is necessary in order to create views using other layers
GRANT ROLE {{ environment }}_transformed_r TO ROLE {{ environment }}_published_owner;
GRANT ROLE {{ environment }}_utils_r TO ROLE {{ environment }}_published_owner;
GRANT ROLE {{ environment }}_ml_r TO ROLE {{ environment }}_published_owner;
-- TODO: This grant should be revoked once dependencies between layers are fixed
-- Each layer should only be dependent on the layer directly below
GRANT ROLE {{ environment }}_raw_r TO ROLE {{ environment }}_published_owner;


-- DDL role has to have owner role in order to modify existing objects
GRANT ROLE {{ environment }}_published_owner TO ROLE {{ environment }}_published_ddl;
