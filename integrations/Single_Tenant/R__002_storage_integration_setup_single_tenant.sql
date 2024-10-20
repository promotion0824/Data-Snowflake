-- ******************************************************************************************************************************
-- Create storage integration for an external stage

-- Note: This script is not driven by Schemachange and it needs to be IDEMPOTENT.
-- ******************************************************************************************************************************
-- This script is called from Data-Core-Dataloader
-- Storage integration
CREATE STORAGE INTEGRATION IF NOT EXISTS ext_all_stages_{{ environment }}_sin
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = AZURE
  ENABLED = TRUE
  AZURE_TENANT_ID = 'd43166d1-c2a1-4f26-a213-f620dba13ab8'
  STORAGE_ALLOWED_LOCATIONS = (
    'azure://{{ stagingStorageAccountName }}.blob.core.windows.net/snowflake-stage/', 
    'azure://{{ stagingStorageAccountName }}.blob.core.windows.net/snowflake-telemetry-stage/', 
    'azure://{{ stagingStorageAccountName }}.blob.core.windows.net/snowflake-adhoc-stage/')
;   

-- Grant permissions to SYSADMIN
GRANT ALL ON INTEGRATION ext_all_stages_{{ environment }}_sin TO SYSADMIN;

-- Grant usage to integrations_user role
GRANT USAGE ON INTEGRATION ext_all_stages_{{ environment }}_sin TO integrations_user;