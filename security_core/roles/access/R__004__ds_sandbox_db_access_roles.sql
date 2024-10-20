-- ------------------------------------------------------------------------------------------------------------------------------
-- Create analytics_db access roles
-- ------------------------------------------------------------------------------------------------------------------------------
USE ROLE USERADMIN;
{% if hasDsSandboxDb -%}

CREATE ROLE IF NOT EXISTS ds_sandbox_all;
CREATE ROLE IF NOT EXISTS ds_sandbox_owner;

USE ROLE {{ defaultRole }};

GRANT USAGE ON DATABASE ds_sandbox_db TO ROLE ds_sandbox_all;
GRANT ALL ON DATABASE ds_sandbox_db TO ROLE ds_sandbox_all;

USE ROLE USERADMIN;
CREATE ROLE IF NOT EXISTS ds_sandbox_published_r;
USE ROLE {{ defaultRole }};

GRANT USAGE ON DATABASE ds_sandbox_db TO ROLE ds_sandbox_published_r;
GRANT USAGE ON SCHEMA ds_sandbox_db.published TO ROLE ds_sandbox_published_r;
GRANT SELECT ON ALL VIEWS IN SCHEMA ds_sandbox_db.published TO ROLE ds_sandbox_published_r;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA ds_sandbox_db.published TO ROLE ds_sandbox_published_r;

-- Owner role
GRANT USAGE ON DATABASE ds_sandbox_db TO ROLE ds_sandbox_owner;

GRANT OWNERSHIP ON ALL SCHEMAS IN DATABASE ds_sandbox_db TO ROLE ds_sandbox_owner COPY CURRENT GRANTS;
GRANT OWNERSHIP ON ALL TABLES IN DATABASE ds_sandbox_db TO ROLE ds_sandbox_owner COPY CURRENT GRANTS;
GRANT OWNERSHIP ON ALL EXTERNAL TABLES IN DATABASE ds_sandbox_db TO ROLE ds_sandbox_owner COPY CURRENT GRANTS;
GRANT OWNERSHIP ON ALL VIEWS IN DATABASE ds_sandbox_db TO ROLE ds_sandbox_owner COPY CURRENT GRANTS;
GRANT OWNERSHIP ON ALL MATERIALIZED VIEWS IN DATABASE ds_sandbox_db TO ROLE ds_sandbox_owner COPY CURRENT GRANTS;
GRANT OWNERSHIP ON ALL STAGES IN DATABASE ds_sandbox_db TO ROLE ds_sandbox_owner COPY CURRENT GRANTS;
GRANT OWNERSHIP ON ALL FILE FORMATS IN DATABASE ds_sandbox_db TO ROLE ds_sandbox_owner COPY CURRENT GRANTS;
GRANT OWNERSHIP ON ALL STREAMS IN DATABASE ds_sandbox_db TO ROLE ds_sandbox_owner COPY CURRENT GRANTS;
GRANT OWNERSHIP ON ALL PROCEDURES IN DATABASE ds_sandbox_db TO ROLE ds_sandbox_owner COPY CURRENT GRANTS;
GRANT OWNERSHIP ON ALL FUNCTIONS IN DATABASE ds_sandbox_db TO ROLE ds_sandbox_owner COPY CURRENT GRANTS;
GRANT OWNERSHIP ON ALL SEQUENCES IN DATABASE ds_sandbox_db TO ROLE ds_sandbox_owner COPY CURRENT GRANTS;
GRANT OWNERSHIP ON ALL TASKS IN DATABASE ds_sandbox_db TO ROLE ds_sandbox_owner COPY CURRENT GRANTS;

-- Before granting ownership on future objects, existing future grants in schema must be removed
SHOW FUTURE GRANTS IN DATABASE ds_sandbox_db;

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
    cmd := 'REVOKE OWNERSHIP ON FUTURE ' || record.object_type || 'S IN DATABASE ds_sandbox_db FROM ROLE ' || record.grantee_name;
    EXECUTE IMMEDIATE cmd;
  END FOR;
  RETURN 'DONE';
END;
$$;

GRANT OWNERSHIP ON FUTURE TABLES IN DATABASE ds_sandbox_db TO ROLE ds_sandbox_owner;
GRANT OWNERSHIP ON FUTURE EXTERNAL TABLES IN DATABASE ds_sandbox_db TO ROLE ds_sandbox_owner;
GRANT OWNERSHIP ON FUTURE VIEWS IN DATABASE ds_sandbox_db TO ROLE ds_sandbox_owner;
GRANT OWNERSHIP ON FUTURE MATERIALIZED VIEWS IN DATABASE ds_sandbox_db TO ROLE ds_sandbox_owner;
GRANT OWNERSHIP ON FUTURE STAGES IN DATABASE ds_sandbox_db TO ROLE ds_sandbox_owner;
GRANT OWNERSHIP ON FUTURE FILE FORMATS IN DATABASE ds_sandbox_db TO ROLE ds_sandbox_owner;
GRANT OWNERSHIP ON FUTURE STREAMS IN DATABASE ds_sandbox_db TO ROLE ds_sandbox_owner;
GRANT OWNERSHIP ON FUTURE PROCEDURES IN DATABASE ds_sandbox_db TO ROLE ds_sandbox_owner;
GRANT OWNERSHIP ON FUTURE FUNCTIONS IN DATABASE ds_sandbox_db TO ROLE ds_sandbox_owner;
GRANT OWNERSHIP ON FUTURE SEQUENCES IN DATABASE ds_sandbox_db TO ROLE ds_sandbox_owner;
GRANT OWNERSHIP ON FUTURE PIPES IN DATABASE ds_sandbox_db TO ROLE ds_sandbox_owner;
GRANT OWNERSHIP ON FUTURE TASKS IN DATABASE ds_sandbox_db TO ROLE ds_sandbox_owner;

GRANT ROLE ds_sandbox_owner TO ROLE ds_sandbox_all;
GRANT ROLE ds_sandbox_owner TO ROLE global_owner;

{%- endif %}

USE ROLE {{ defaultRole }}; 