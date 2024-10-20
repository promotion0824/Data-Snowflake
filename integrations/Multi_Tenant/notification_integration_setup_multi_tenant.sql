!set variable_substitution=true
!set timing=true
!set echo=true
-- Notification integration
CREATE NOTIFICATION INTEGRATION IF NOT EXISTS ext_&{int_name}_&{environment}_nin
  ENABLED = TRUE
  TYPE = QUEUE
  NOTIFICATION_PROVIDER = AZURE_STORAGE_QUEUE
  AZURE_TENANT_ID = '&{azure_tenant_id}'  
  AZURE_STORAGE_QUEUE_PRIMARY_URI = 'https://&{storage_account_name}.queue.core.windows.net/&{queue_name}'
;

-- Grant permissions to SYSADMIN
GRANT ALL ON INTEGRATION ext_&{int_name}_&{environment}_nin TO SYSADMIN;

-- Grant usage to integrations_user role
GRANT USAGE ON INTEGRATION ext_&{int_name}_&{environment}_nin TO integrations_user;