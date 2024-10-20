-- ------------------------------------------------------------------------------------------------------------------------------
-- Create app_dashboards schemas if not exists
-- ------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE WAREHOUSE app_dashboards_{{ environment }}_wh 
WITH 
	WAREHOUSE_SIZE = 'XSMALL' 
	AUTO_SUSPEND = 60 
	AUTO_RESUME = TRUE 
	MIN_CLUSTER_COUNT = 1
	SCALING_POLICY = 'STANDARD' 
	INITIALLY_SUSPENDED = TRUE
	COMMENT = '';