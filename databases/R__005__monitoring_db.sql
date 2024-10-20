-- ------------------------------------------------------------------------------------------------------------------------------
-- Create database for monitoring data
-- ------------------------------------------------------------------------------------------------------------------------------

USE ROLE {{ defaultRole }};

{% if hasMonitoringDb -%}

CREATE DATABASE IF NOT EXISTS monitoring_db;

CREATE SCHEMA IF NOT EXISTS monitoring_db.transformed;

CREATE SCHEMA IF NOT EXISTS monitoring_db.published;

{%- endif %}

SELECT 1;