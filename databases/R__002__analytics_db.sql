-- ------------------------------------------------------------------------------------------------------------------------------
-- Create analytics_db database
-- This database is used for Sigma write-back and materialization
-- ------------------------------------------------------------------------------------------------------------------------------

USE ROLE {{ defaultRole }};

{% if hasAnalyticsDb -%}

CREATE DATABASE IF NOT EXISTS analytics_db;

{%- endif %}

SELECT 1;