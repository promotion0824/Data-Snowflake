-- ------------------------------------------------------------------------------------------------------------------------------
-- Create schemas if not exists
-- ------------------------------------------------------------------------------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS {{ environment }}_db.raw;
CREATE SCHEMA IF NOT EXISTS {{ environment }}_db.published;
CREATE SCHEMA IF NOT EXISTS {{ environment }}_db.transformed;

CREATE SCHEMA IF NOT EXISTS {{ environment }}_db.schemachange;
CREATE SCHEMA IF NOT EXISTS {{ environment }}_db.utils;
CREATE SCHEMA IF NOT EXISTS {{ environment }}_db.app_dashboards;

-- ------------------------------------------------------------------------------------------------------------------------------
-- Monitoring database
-- ------------------------------------------------------------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS monitoring_db.raw;
CREATE SCHEMA IF NOT EXISTS monitoring_db.transformed;
CREATE SCHEMA IF NOT EXISTS monitoring_db.published;

-- Deploy only for Willow AU internal account
{% if accountType == 'internal' and customerName == 'wil' and azureRegionIdentifier == 'aue1' -%}

CREATE SCHEMA IF NOT EXISTS central_monitoring_db.raw;
CREATE SCHEMA IF NOT EXISTS central_monitoring_db.transformed;
CREATE SCHEMA IF NOT EXISTS central_monitoring_db.published;

{%- endif %}

SELECT 1;
