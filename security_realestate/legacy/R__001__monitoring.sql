-- ------------------------------------------------------------------------------------------------------------------------------
-- Legacy roles and users
-- These roles and users will be eventually replaced and decommissioned
-- ------------------------------------------------------------------------------------------------------------------------------
-- ------------------------------------------------------------------------------------------------------------------------------
-- Monitoring pipeline
-- ------------------------------------------------------------------------------------------------------------------------------
{% if accountType == 'customer' -%}

USE ROLE {{ defaultRole }};

GRANT USAGE ON DATABASE {{ environment }}_db TO ROLE monitoring_pipeline_reader;

-- This is needed in order to allow to monitor pipes
GRANT USAGE ON SCHEMA {{ environment }}_db.raw  TO ROLE monitoring_pipeline_reader;

{%- endif %}

USE ROLE {{ defaultRole }};