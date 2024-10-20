-- ******************************************************************************************************************************
-- Create storage integration for an external stage

-- Note: This script is not driven by Schemachange and it needs to be IDEMPOTENT.
-- ******************************************************************************************************************************
-- This script is called from Data-Core-Dataloader
!set variable_substitution=true
!set timing=true
!set echo=true

-- Storage integration
CREATE STORAGE INTEGRATION IF NOT EXISTS ext_all_stages_&{environment}_sin
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = AZURE
  ENABLED = TRUE
  AZURE_TENANT_ID = '&{azure_tenant_id}'
  STORAGE_ALLOWED_LOCATIONS = (
    'azure://&{storage_account_name}.blob.core.windows.net/snowflake-stage/', 
    'azure://&{storage_account_name}.blob.core.windows.net/snowflake-telemetry-stage/', 
    'azure://&{storage_account_name}.blob.core.windows.net/snowflake-adhoc-stage/')
;   

-- Grant permissions to SYSADMIN
GRANT ALL ON INTEGRATION ext_all_stages_&{environment}_sin TO SYSADMIN;

-- Grant usage to integrations_user role
GRANT USAGE ON INTEGRATION ext_all_stages_&{environment}_sin TO integrations_user;