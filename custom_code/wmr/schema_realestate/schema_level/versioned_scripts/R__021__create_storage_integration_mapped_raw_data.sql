-- ******************************************************************************************************************************
-- Create storage integration for an external stage for mapped raw data.
-- This was created as a bespoke storage integration since it needs its own STORAGE_ALLOWED_LOCATIONS value
-- ******************************************************************************************************************************

CREATE OR REPLACE STORAGE INTEGRATION ext_stage_mapped_raw_data_{{ environment }}_sin
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = AZURE
  ENABLED = TRUE
  AZURE_TENANT_ID = 'd43166d1-c2a1-4f26-a213-f620dba13ab8'
  STORAGE_ALLOWED_LOCATIONS = (
    'azure://{{ stagingStorageAccountName }}.blob.core.windows.net/eh-capture-container/')
;   

-- Grant permissions to SYSADMIN
GRANT ALL ON INTEGRATION ext_stage_mapped_raw_data_{{ environment }}_sin TO SYSADMIN;

-- Grant usage to integrations_user role
GRANT USAGE ON INTEGRATION ext_stage_mapped_raw_data_{{ environment }}_sin TO integrations_user;