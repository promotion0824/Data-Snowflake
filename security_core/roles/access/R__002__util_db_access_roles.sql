-- ------------------------------------------------------------------------------------------------------------------------------
-- Create util_db access roles
-- ------------------------------------------------------------------------------------------------------------------------------
USE ROLE USERADMIN;

-- ------------------------------------------------------------------------------------------------------------------------------
-- PUBLIC schema
-- ------------------------------------------------------------------------------------------------------------------------------

CREATE ROLE IF NOT EXISTS util_public_r;
CREATE ROLE IF NOT EXISTS util_public_w;
CREATE ROLE IF NOT EXISTS util_public_ddl;
CREATE ROLE IF NOT EXISTS util_public_owner;

USE ROLE {{ defaultRole }};

-- Reader role
GRANT USAGE ON DATABASE util_db TO ROLE util_public_r;
GRANT USAGE ON SCHEMA util_db.public TO ROLE util_public_r;
GRANT SELECT ON ALL TABLES IN SCHEMA util_db.public TO ROLE util_public_r;
GRANT SELECT ON ALL VIEWS IN SCHEMA util_db.public TO ROLE util_public_r;
GRANT SELECT ON FUTURE TABLES IN SCHEMA util_db.public TO ROLE util_public_r;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA util_db.public TO ROLE util_public_r;

-- Writer role
GRANT USAGE ON DATABASE util_db TO ROLE util_public_w;
GRANT USAGE ON SCHEMA util_db.public TO ROLE util_public_w;
GRANT INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA util_db.public TO ROLE util_public_w;
GRANT INSERT, UPDATE, DELETE, TRUNCATE ON FUTURE TABLES IN SCHEMA util_db.public TO ROLE util_public_w;

-- DDL role
GRANT USAGE ON DATABASE util_db TO ROLE util_public_ddl;
GRANT USAGE ON SCHEMA util_db.public TO ROLE util_public_ddl;
GRANT CREATE TABLE ON SCHEMA util_db.public TO ROLE util_public_ddl;
GRANT CREATE VIEW ON SCHEMA util_db.public TO ROLE util_public_ddl;

-- Owner role
GRANT USAGE ON DATABASE util_db TO ROLE util_public_owner;
GRANT USAGE ON SCHEMA util_db.public TO ROLE util_public_owner;
GRANT OWNERSHIP ON ALL TABLES IN SCHEMA util_db.public TO ROLE util_public_owner COPY CURRENT GRANTS;
GRANT OWNERSHIP ON ALL VIEWS IN SCHEMA util_db.public TO ROLE util_public_owner COPY CURRENT GRANTS;

-- Before granting ownership on future objects, existing future grants in schema must be removed
SHOW FUTURE GRANTS IN SCHEMA util_db.public;

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
    cmd := 'REVOKE OWNERSHIP ON FUTURE ' || record.object_type || 'S IN SCHEMA util_db.public FROM ROLE ' || record.grantee_name;
    EXECUTE IMMEDIATE cmd;
  END FOR;
  RETURN 'DONE';
END;
$$;

GRANT OWNERSHIP ON FUTURE TABLES IN SCHEMA util_db.public TO ROLE util_public_owner;
GRANT OWNERSHIP ON FUTURE VIEWS IN SCHEMA util_db.public TO ROLE util_public_owner;

GRANT ROLE util_public_owner TO ROLE util_public_ddl;
GRANT ROLE util_public_owner TO ROLE global_owner;

-- ------------------------------------------------------------------------------------------------------------------------------
-- schemachange schema
-- ------------------------------------------------------------------------------------------------------------------------------

CREATE ROLE IF NOT EXISTS util_schemachange_r;
CREATE ROLE IF NOT EXISTS util_schemachange_w;
CREATE ROLE IF NOT EXISTS util_schemachange_ddl;
CREATE ROLE IF NOT EXISTS util_schemachange_owner;

-- Reader role
GRANT USAGE ON DATABASE util_db TO ROLE util_schemachange_r;
GRANT USAGE ON SCHEMA util_db.schemachange TO ROLE util_schemachange_r;
GRANT SELECT ON ALL TABLES IN SCHEMA util_db.schemachange TO ROLE util_schemachange_r;
GRANT SELECT ON ALL VIEWS IN SCHEMA util_db.schemachange TO ROLE util_schemachange_r;
GRANT SELECT ON FUTURE TABLES IN SCHEMA util_db.schemachange TO ROLE util_schemachange_r;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA util_db.schemachange TO ROLE util_schemachange_r;

-- Writer role
GRANT USAGE ON DATABASE util_db TO ROLE util_schemachange_w;
GRANT USAGE ON SCHEMA util_db.schemachange TO ROLE util_schemachange_w;
GRANT INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA util_db.schemachange TO ROLE util_schemachange_w;
GRANT INSERT, UPDATE, DELETE, TRUNCATE ON FUTURE TABLES IN SCHEMA util_db.schemachange TO ROLE util_schemachange_w;

-- DDL role
GRANT USAGE ON DATABASE util_db TO ROLE util_schemachange_ddl;
GRANT USAGE ON SCHEMA util_db.schemachange TO ROLE util_schemachange_ddl;
GRANT CREATE TABLE ON SCHEMA util_db.schemachange TO ROLE util_schemachange_ddl;
GRANT CREATE VIEW ON SCHEMA util_db.schemachange TO ROLE util_schemachange_ddl;

-- Owner role
GRANT USAGE ON DATABASE util_db TO ROLE util_schemachange_owner;
GRANT USAGE ON SCHEMA util_db.schemachange TO ROLE util_schemachange_owner;
GRANT OWNERSHIP ON ALL EXTERNAL TABLES IN SCHEMA util_db.schemachange TO ROLE util_schemachange_owner COPY CURRENT GRANTS;
GRANT OWNERSHIP ON ALL VIEWS IN SCHEMA util_db.schemachange TO ROLE util_schemachange_owner COPY CURRENT GRANTS;

-- Before granting ownership on future objects, existing future grants in schema must be removed
SHOW FUTURE GRANTS IN SCHEMA util_db.schemachange;

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
    cmd := 'REVOKE OWNERSHIP ON FUTURE ' || record.object_type || 'S IN SCHEMA util_db.schemachange FROM ROLE ' || record.grantee_name;
    EXECUTE IMMEDIATE cmd;
  END FOR;
  RETURN 'DONE';
END;
$$;

GRANT OWNERSHIP ON FUTURE TABLES IN SCHEMA util_db.schemachange TO ROLE util_schemachange_owner;
GRANT OWNERSHIP ON FUTURE VIEWS IN SCHEMA util_db.schemachange TO ROLE util_schemachange_owner;

GRANT ROLE util_schemachange_owner TO ROLE util_schemachange_ddl;
GRANT ROLE util_schemachange_owner TO ROLE global_owner;

USE ROLE {{ defaultRole }};