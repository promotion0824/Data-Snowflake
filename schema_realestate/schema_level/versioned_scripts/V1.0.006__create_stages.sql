-- ******************************************************************************************************************************
-- Create external stages for the Data Loader
-- ******************************************************************************************************************************
USE ROLE {{ defaultRole }};

USE DATABASE {{ environment }}_db;

-- Create adhoc internal stage
--CREATE STAGE IF NOT EXISTS raw.adhoc_csv_sg  FILE_FORMAT = raw.csvgz_ff;

{% if accountType == 'customer' -%}

--{% if deploymentType == 'single-tenant' -%}
-- Create generic Data Loader external stage
CREATE STAGE IF NOT EXISTS raw.data_loader_esg
  URL = 'azure://{{ stagingStorageAccountName }}.blob.core.windows.net/snowflake-stage/'
  STORAGE_INTEGRATION = ext_all_stages_{{ environment }}_sin,
  FILE_FORMAT = raw.json_ff;

-- Create adhoc external stage
CREATE STAGE IF NOT EXISTS raw.adhoc_esg
  URL = 'azure://{{ stagingStorageAccountName }}.blob.core.windows.net/snowflake-adhoc-stage/'
  STORAGE_INTEGRATION = ext_all_stages_{{ environment }}_sin;

-- TODO: Replace this with correctly named integration
 -- Create Data Loader Telemetry external stage 
CREATE STAGE IF NOT EXISTS raw.data_loader_telemetry_esg
  URL = 'azure://{{ stagingStorageAccountName }}.blob.core.windows.net/snowflake-telemetry-stage/'
  STORAGE_INTEGRATION = ext_all_stages_{{ environment }}_sin,
  FILE_FORMAT = raw.csvgz_ff;

-- {% else %}

-- CREATE STAGE IF NOT EXISTS raw.data_loader_esg
--   URL = 'azure://wilsfstg{{ customerName }}{{ environment }}dls{{ azureRegionIdentifier }}.blob.core.windows.net/{{ customerName }}-{{ accountName }}-stage/'
--   STORAGE_INTEGRATION = ext_stage_{{ environment }}_sin,
--   FILE_FORMAT = raw.json_ff;

-- CREATE STAGE IF NOT EXISTS raw.adhoc_esg
--   URL = 'azure://wilsfstg{{ customerName }}{{ environment }}dls{{ azureRegionIdentifier }}.blob.core.windows.net/{{ customerName }}-{{ accountName }}-adhoc-stage/'
--   STORAGE_INTEGRATION = ext_stage_adhoc_{{ environment }}_sin;

-- CREATE STAGE IF NOT EXISTS raw.data_loader_telemetry_esg
--   URL = 'azure://wilsfstg{{ customerName }}{{ environment }}dls{{ azureRegionIdentifier }}.blob.core.windows.net/{{ customerName }}-{{ accountName }}-telemetry-stage/'
--   STORAGE_INTEGRATION = ext_telemetry_stage_{{ environment }}_sin,
--   FILE_FORMAT = raw.csvgz_ff;

{%- endif %}
--{%- endif %}

USE ROLE {{ defaultRole }};