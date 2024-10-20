-- ******************************************************************************************************************************
-- Create notification integrations for an external stage

-- Note: This script is not driven by Schemachange and it needs to be IDEMPOTENT.
-- ******************************************************************************************************************************
-- This script is called from Data-Core-Dataloader

-- Stage notification integration
CREATE NOTIFICATION INTEGRATION IF NOT EXISTS ext_stage_{{ environment }}_nin
  ENABLED = TRUE
  TYPE = QUEUE
  NOTIFICATION_PROVIDER = AZURE_STORAGE_QUEUE
  AZURE_TENANT_ID = 'd43166d1-c2a1-4f26-a213-f620dba13ab8'
  AZURE_STORAGE_QUEUE_PRIMARY_URI = 'https://{{ stagingStorageAccountName }}.queue.core.windows.net/sf-stage-notification-queue'
;

-- Grant permissions to SYSADMIN
GRANT ALL ON INTEGRATION ext_stage_{{ environment }}_nin TO SYSADMIN;

-- Grant usage to integrations_user role
GRANT USAGE ON INTEGRATION ext_stage_{{ environment }}_nin TO integrations_user;

-- Telemetry stage notification integration
CREATE NOTIFICATION INTEGRATION IF NOT EXISTS ext_telemetry_stage_{{ environment }}_nin
  ENABLED = TRUE
  TYPE = QUEUE
  NOTIFICATION_PROVIDER = AZURE_STORAGE_QUEUE
  AZURE_TENANT_ID = 'd43166d1-c2a1-4f26-a213-f620dba13ab8'
  AZURE_STORAGE_QUEUE_PRIMARY_URI = 'https://{{ stagingStorageAccountName }}.queue.core.windows.net/sf-telemetry-stage-notification-queue'
;

-- Grant permissions to SYSADMIN
GRANT ALL ON INTEGRATION ext_telemetry_stage_{{ environment }}_nin TO SYSADMIN;

-- Grant usage to integrations_user role
GRANT USAGE ON INTEGRATION ext_telemetry_stage_{{ environment }}_nin TO integrations_user;

