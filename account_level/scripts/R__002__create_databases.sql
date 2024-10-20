-- ------------------------------------------------------------------------------------------------------------------------------
-- Create main <environment>_db
-- ------------------------------------------------------------------------------------------------------------------------------
{% if accountType == 'customer' -%}

CREATE DATABASE IF NOT EXISTS {{ environment }}_db;

{%- endif %}

-- ------------------------------------------------------------------------------------------------------------------------------
-- Create analytics_db that allows for writing from Sigma
-- ------------------------------------------------------------------------------------------------------------------------------
-- Moved to Data-Core-Snowflake-Account pipeline
-- {% if accountType == 'customer' -%}

-- CREATE DATABASE IF NOT EXISTS analytics_db;

-- {%- endif %}

-- ------------------------------------------------------------------------------------------------------------------------------
-- Database for logs and monitoring
-- ------------------------------------------------------------------------------------------------------------------------------
-- CREATE DATABASE IF NOT EXISTS monitoring_db;

-- Deploy only for Willow AU internal account
{% if accountType == 'internal' and customerName == 'wil' and azureRegionIdentifier == 'aue1' -%}

CREATE DATABASE IF NOT EXISTS central_monitoring_db;

{%- endif %}

SELECT 1;