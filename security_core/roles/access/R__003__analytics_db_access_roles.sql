-- ------------------------------------------------------------------------------------------------------------------------------
-- Create analytics_db access roles
-- ------------------------------------------------------------------------------------------------------------------------------

USE ROLE USERADMIN;
{% if hasAnalyticsDb -%}

CREATE ROLE IF NOT EXISTS analytics_public_r;
CREATE ROLE IF NOT EXISTS analytics_public_w;
CREATE ROLE IF NOT EXISTS analytics_public_ddl;
CREATE ROLE IF NOT EXISTS analytics_public_owner;

USE ROLE {{ defaultRole }};

-- Reader role
GRANT USAGE ON DATABASE analytics_db TO ROLE analytics_public_r;
GRANT USAGE ON SCHEMA analytics_db.public TO ROLE analytics_public_r;
GRANT SELECT ON ALL TABLES IN SCHEMA analytics_db.public TO ROLE analytics_public_r;
GRANT SELECT ON ALL VIEWS IN SCHEMA analytics_db.public TO ROLE analytics_public_r;
GRANT SELECT ON FUTURE TABLES IN SCHEMA analytics_db.public TO ROLE analytics_public_r;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA analytics_db.public TO ROLE analytics_public_r;

-- Writer role
GRANT USAGE ON DATABASE analytics_db TO ROLE analytics_public_w;
GRANT USAGE ON SCHEMA analytics_db.public TO ROLE analytics_public_w;
GRANT INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA analytics_db.public TO ROLE analytics_public_w;
GRANT INSERT, UPDATE, DELETE, TRUNCATE ON FUTURE TABLES IN SCHEMA analytics_db.public TO ROLE analytics_public_w;

-- DDL role for Sigma write-back
GRANT USAGE ON DATABASE analytics_db TO ROLE analytics_public_ddl;
GRANT USAGE ON SCHEMA analytics_db.public TO ROLE analytics_public_ddl;
GRANT CREATE TABLE ON SCHEMA analytics_db.public TO ROLE analytics_public_ddl;
GRANT CREATE VIEW ON SCHEMA analytics_db.public TO ROLE analytics_public_ddl;

-- Owner role
GRANT USAGE ON DATABASE analytics_db TO ROLE analytics_public_owner;
GRANT USAGE ON SCHEMA analytics_db.public TO ROLE analytics_public_owner;
GRANT OWNERSHIP ON ALL EXTERNAL TABLES IN SCHEMA analytics_db.public TO ROLE analytics_public_owner COPY CURRENT GRANTS;
GRANT OWNERSHIP ON ALL VIEWS IN SCHEMA analytics_db.public TO ROLE analytics_public_owner COPY CURRENT GRANTS;

-- Before granting ownership on future objects, existing future grants in schema must be removed
SHOW FUTURE GRANTS IN SCHEMA analytics_db.public;

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
    cmd := 'REVOKE OWNERSHIP ON FUTURE ' || record.object_type || 'S IN SCHEMA analytics_db.public FROM ROLE ' || record.grantee_name;
    EXECUTE IMMEDIATE cmd;
  END FOR;
  RETURN 'DONE';
END;
$$;

GRANT OWNERSHIP ON FUTURE TABLES IN SCHEMA analytics_db.public TO ROLE analytics_public_owner;
GRANT OWNERSHIP ON FUTURE VIEWS IN SCHEMA analytics_db.public TO ROLE analytics_public_owner;

GRANT ROLE analytics_public_owner TO ROLE analytics_public_ddl;
GRANT ROLE analytics_public_owner TO ROLE global_owner;

{%- endif %}

USE ROLE {{ defaultRole }};