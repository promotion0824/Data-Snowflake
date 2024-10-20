-- ******************************************************************************************************************************
-- Create external stages for the Data Loader
-- ******************************************************************************************************************************
USE ROLE {{ defaultRole }};

USE DATABASE {{ environment }}_db;


-- Create generic Data Loader external stage
CREATE OR REPLACE STAGE raw.data_loader_eh_capture_container
  URL = 'azure://{{ stagingStorageAccountName }}.blob.core.windows.net/eh-capture-container/'
  STORAGE_INTEGRATION = ext_stage_mapped_raw_data_{{ environment }}_sin;

USE ROLE {{ defaultRole }};