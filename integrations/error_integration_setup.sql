-- ******************************************************************************************************************************
-- Create notification integrations for routing errors to Azure Event Grid

-- Note: This script is not driven by Schemachange.
-- It is safe to replace existing integration since this is run only by Data-Snowflake-ErrorIntegration.
-- ******************************************************************************************************************************

!set variable_substitution=true
!set timing=true
!set echo=true
 
-- Notification integration
CREATE OR REPLACE NOTIFICATION INTEGRATION error_&{environment}_nin
  ENABLED = TRUE
  TYPE = QUEUE
  NOTIFICATION_PROVIDER = AZURE_EVENT_GRID
  DIRECTION = OUTBOUND
  AZURE_EVENT_GRID_TOPIC_ENDPOINT = '&{event_grid_topic_endpoint}'
  AZURE_TENANT_ID = '&{azure_tenant_id}'
;

-- Grant permissions to SYSADMIN
GRANT ALL ON INTEGRATION error_&{environment}_nin TO SYSADMIN;

-- Grant usage to integrations_user role
GRANT USAGE ON INTEGRATION error_&{environment}_nin TO integrations_user;