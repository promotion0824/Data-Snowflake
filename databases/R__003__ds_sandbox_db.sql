-- ------------------------------------------------------------------------------------------------------------------------------
-- Create ds_sandbox_db database
-- This database is used as Data Science Sandbox
-- ------------------------------------------------------------------------------------------------------------------------------

USE ROLE {{ defaultRole }};

{% if hasDsSandboxDb -%}

CREATE DATABASE IF NOT EXISTS ds_sandbox_db;

CREATE SCHEMA IF NOT EXISTS ds_sandbox_db.published;

{%- endif %}

SELECT 1;