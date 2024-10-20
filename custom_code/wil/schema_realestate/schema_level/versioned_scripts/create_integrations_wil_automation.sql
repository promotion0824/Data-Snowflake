- ******************************************************************************************************************************
-- Create external stages - custom
-- can't create more notification integrations - commenting out for prd deployment.
-- ******************************************************************************************************************************
-- ------------------------------------------------------------------------------------------------------------------------------
-- wo77920 can't create any more notification integrations 
-- pulling this from automated deployment for now
-- ------------------------------------------------------------------------------------------------------------------------------

USE ROLE {{ defaultRole }};
USE SCHEMA wil_automation.utils;

-- Create adhoc external stage - for wil_automation folder
CREATE STAGE IF NOT EXISTS wil_automation_{{ environment }}_csv_esg
  URL = 'azure://wilsfstg{{ customerName }}{{ environment }}dls{{ azureRegionIdentifier }}.blob.core.windows.net/{{ customerName }}-{{ accountName }}-adhoc-stage'
  STORAGE_INTEGRATION = ext_stage_adhoc_{{ environment }}_sin,
  FILE_FORMAT = csv_ff;

USE ROLE ACCOUNTADMIN;
-- is this notification too broad at the customer level instead of the wil_automation folder?
CREATE NOTIFICATION INTEGRATION IF NOT EXISTS ext_stage_adhoc_{{ environment }}_wil_automation_nin
  ENABLED = TRUE
  TYPE = QUEUE
  NOTIFICATION_PROVIDER = AZURE_STORAGE_QUEUE
  AZURE_STORAGE_QUEUE_PRIMARY_URI = 'https://wilsfstg{{ customerName }}{{ environment }}dls{{ azureRegionIdentifier }}.queue.core.windows.net/{{ customerName }}-{{ accountName }}-adhoc-notification-queue'
  AZURE_TENANT_ID = 'd43166d1-c2a1-4f26-a213-f620dba13ab8'
;
GRANT ALL ON INTEGRATION ext_stage_adhoc_{{ environment }}_wil_automation_nin TO SYSADMIN;

USE ROLE ACCOUNTADMIN;
ALTER PIPE IF EXISTS ingest_adhoc_wil_automation_pp 
  SET PIPE_EXECUTION_PAUSED = true;

DROP PIPE IF EXISTS ingest_adhoc_wil_automation_pp;  

CREATE OR REPLACE PIPE ingest_adhoc_wil_automation_pp
	AUTO_INGEST = TRUE
	INTEGRATION = 'EXT_STAGE_ADHOC_{{ uppercaseEnvironment }}_WIL_AUTOMATION_NIN'
  AS
 -- We just need the file names; we will create views directly against the parquet files 
 COPY INTO data_compliance.adhoc_stage_pipe_csv 
	 FROM (
			SELECT distinct metadata$filename,'',current_timestamp()
				FROM @utils.wil_automation_dev_csv_esg/wil_automation/ (FILE_FORMAT => 'utils.parquet_ff', PATTERN=>'.*(parquet)$')
		  );

ALTER PIPE IF EXISTS ingest_adhoc_wil_automation_pp 
  SET PIPE_EXECUTION_PAUSED = false;
GRANT MONITOR ON pipe ingest_adhoc_wil_automation_pp TO ROLE SYSADMIN;

USE ROLE {{ defaultRole }};
CREATE OR REPLACE STREAM data_compliance.adhoc_stage_pipe_csv_str
    ON TABLE data_compliance.adhoc_stage_pipe_csv
    APPEND_ONLY = TRUE
    SHOW_INITIAL_ROWS = TRUE;
