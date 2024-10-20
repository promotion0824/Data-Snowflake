-- ------------------------------------------------------------------------------------------------------------------------------
-- Legacy roles and users
-- These roles and users will be eventually replaced and decommissioned
-- ------------------------------------------------------------------------------------------------------------------------------
-- ------------------------------------------------------------------------------------------------------------------------------
-- Monitoring pipeline user
-- ------------------------------------------------------------------------------------------------------------------------------
USE ROLE USERADMIN;

CREATE USER IF NOT EXISTS monitoring_pipeline_usr
  LOGIN_NAME   = 'monitoring_pipeline_usr'
  DEFAULT_ROLE = monitoring
  DEFAULT_WAREHOUSE = monitoring_pipeline_wh
  MUST_CHANGE_PASSWORD = TRUE; 

-- ------------------------------------------------------------------------------------------------------------------------------
-- Monitoring pipeline reader role
-- (reads logs and other monitoring data from all customer accounts)
-- ------------------------------------------------------------------------------------------------------------------------------
USE ROLE USERADMIN;
CREATE ROLE IF NOT EXISTS monitoring_pipeline_reader;

GRANT ROLE monitoring_pipeline_reader TO USER monitoring_pipeline_usr;
GRANT ROLE monitoring_pipeline_reader TO ROLE SYSADMIN;

USE ROLE {{ defaultRole }};
GRANT MONITOR USAGE ON ACCOUNT TO ROLE monitoring_pipeline_reader;
GRANT MONITOR EXECUTION ON ACCOUNT TO ROLE monitoring_pipeline_reader;

-- The ADF reader needs select on all and future tables and views
-- and also permissions to create stage in raw schema
USE ROLE {{ defaultRole }};

GRANT USAGE ON DATABASE monitoring_db TO ROLE monitoring_pipeline_reader;
GRANT USAGE ON SCHEMA monitoring_db.transformed TO ROLE monitoring_pipeline_reader;
GRANT SELECT ON ALL TABLES IN schema monitoring_db.transformed TO ROLE monitoring_pipeline_reader;
GRANT SELECT ON ALL VIEWS  IN schema monitoring_db.transformed TO ROLE monitoring_pipeline_reader;
GRANT SELECT ON FUTURE TABLES IN schema monitoring_db.transformed TO ROLE monitoring_pipeline_reader;
GRANT SELECT ON FUTURE VIEWS  IN schema monitoring_db.transformed TO ROLE monitoring_pipeline_reader;

GRANT USAGE ON SCHEMA monitoring_db.published TO ROLE monitoring_pipeline_reader;
GRANT SELECT ON ALL VIEWS  IN schema monitoring_db.published TO ROLE monitoring_pipeline_reader;
GRANT SELECT ON FUTURE VIEWS  IN schema monitoring_db.published TO ROLE monitoring_pipeline_reader;

GRANT USAGE ON WAREHOUSE monitoring_pipeline_wh TO ROLE monitoring_pipeline_reader;
GRANT OPERATE ON WAREHOUSE monitoring_pipeline_wh TO ROLE monitoring_pipeline_reader;

GRANT USAGE ON DATABASE _{{ customerName }} TO ROLE monitoring_pipeline_reader;

GRANT IMPORTED PRIVILEGES ON DATABASE snowflake TO ROLE monitoring_pipeline_reader;


-- Deploy only for Willow AU internal account
-- {% if accountName == 'wilaue1' -%}

-- -- ------------------------------------------------------------------------------------------------------------------------------
-- -- Monitoring pipeline writer role
-- -- (writes logs and other monitoring data into the central Willow account)
-- -- ------------------------------------------------------------------------------------------------------------------------------
-- USE ROLE USERADMIN;

-- CREATE ROLE IF NOT EXISTS monitoring_pipeline_writer;

-- USE ROLE {{ defaultRole }};

-- GRANT ROLE monitoring_pipeline_writer TO USER monitoring_pipeline_usr;
-- GRANT ROLE monitoring_pipeline_writer TO ROLE SYSADMIN;

-- -- The ADF writer needs select, insert on all and future tables in raw schema
-- -- and also permissions to create stage in raw schema
-- GRANT USAGE ON DATABASE central_monitoring_db TO ROLE monitoring_pipeline_writer;
-- GRANT USAGE ON SCHEMA central_monitoring_db.raw TO ROLE monitoring_pipeline_writer;

-- GRANT INSERT ON ALL TABLES IN schema central_monitoring_db.raw TO ROLE monitoring_pipeline_writer;
-- GRANT SELECT ON ALL TABLES IN schema central_monitoring_db.raw TO ROLE monitoring_pipeline_writer;

-- GRANT INSERT ON FUTURE TABLES IN SCHEMA central_monitoring_db.raw TO ROLE monitoring_pipeline_writer;
-- GRANT SELECT ON FUTURE TABLES IN SCHEMA central_monitoring_db.raw TO ROLE monitoring_pipeline_writer;

-- GRANT CREATE STAGE ON SCHEMA central_monitoring_db.raw TO ROLE monitoring_pipeline_writer;

-- -- It has a dedicated warehouse
-- GRANT USAGE ON WAREHOUSE monitoring_pipeline_wh TO ROLE monitoring_pipeline_writer;
-- GRANT OPERATE ON WAREHOUSE monitoring_pipeline_wh TO ROLE monitoring_pipeline_writer;

-- -- ------------------------------------------------------------------------------------------------------------------------------
-- -- Monitoring dashboard user and role
-- -- ------------------------------------------------------------------------------------------------------------------------------
-- USE ROLE USERADMIN;

-- CREATE USER IF NOT EXISTS monitoring_dashboards_usr
--   LOGIN_NAME   = 'monitoring_dashboards_usr'
--   DEFAULT_ROLE = monitoring_dashboard
--   DEFAULT_WAREHOUSE = monitoring_dashboard_wh
--   DEFAULT_NAMESPACE = 'monitoring_db.published'
--   MUST_CHANGE_PASSWORD = TRUE; 

-- CREATE ROLE IF NOT EXISTS monitoring_dashboard;

-- USE ROLE {{ defaultRole }};

-- GRANT ROLE monitoring_dashboard TO USER monitoring_dashboards_usr ;

-- GRANT USAGE ON DATABASE central_monitoring_db TO ROLE monitoring_dashboard;
-- GRANT USAGE ON SCHEMA central_monitoring_db.published TO ROLE monitoring_dashboard;

-- GRANT SELECT ON ALL TABLES IN SCHEMA central_monitoring_db.published TO ROLE monitoring_dashboard;
-- GRANT SELECT ON ALL VIEWS  IN SCHEMA central_monitoring_db.published TO ROLE monitoring_dashboard;
-- GRANT SELECT ON FUTURE TABLES IN SCHEMA central_monitoring_db.published TO ROLE monitoring_dashboard;
-- GRANT SELECT ON FUTURE VIEWS  IN SCHEMA central_monitoring_db.published TO ROLE monitoring_dashboard;

-- GRANT USAGE ON WAREHOUSE monitoring_dashboard_wh TO ROLE monitoring_dashboard;
-- GRANT OPERATE ON WAREHOUSE monitoring_dashboard_wh TO ROLE monitoring_dashboard;
-- GRANT MONITOR ON WAREHOUSE monitoring_dashboard_wh TO ROLE monitoring_dashboard;

-- -- To see the cost for the account
-- GRANT MONITOR USAGE ON ACCOUNT TO ROLE monitoring_dashboard;
-- {%- endif %}

USE ROLE {{ defaultRole }};
