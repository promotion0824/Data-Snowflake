-- ------------------------------------------------------------------------------------------------------------------------------
-- Create database for sustainability integration
-- ------------------------------------------------------------------------------------------------------------------------------

USE ROLE {{ defaultRole }};

{% if hasSustainabilityDb -%}

CREATE DATABASE IF NOT EXISTS sustainability_db;

CREATE SCHEMA IF NOT EXISTS sustainability_db.published;

{%- endif %}

SELECT 1;