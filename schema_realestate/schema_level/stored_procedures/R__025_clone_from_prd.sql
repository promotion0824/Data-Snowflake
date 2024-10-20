-- ******************************************************************************************************************************
-- Stored procedure to clone data from prd to uat
-- ******************************************************************************************************************************
EXECUTE IMMEDIATE
$$
DECLARE 
	currentdb STRING;
BEGIN
	SELECT CURRENT_DATABASE() INTO :currentdb From dual;
	IF (currentdb='UAT_DB') THEN
		CREATE OR REPLACE PROCEDURE transformed.clone_from_prd_sp()
            RETURNS STRING
			LANGUAGE SQL
		AS
        '
			BEGIN
				CREATE OR REPLACE TABLE transformed.telemetry CLONE PRD_DB.transformed.telemetry;
				CREATE OR REPLACE TABLE transformed.twins CLONE PRD_DB.transformed.twins;
				CREATE OR REPLACE TABLE transformed.twins_relationships CLONE PRD_DB.transformed.twins_relationships;
				CREATE OR REPLACE TRANSIENT TABLE transformed.capabilities_assets CLONE PRD_DB.transformed.capabilities_assets;
				CREATE OR REPLACE STREAM transformed.telemetry_str ON TABLE transformed.telemetry APPEND_ONLY = TRUE;
				CREATE OR REPLACE STREAM transformed.transformed.comfort_telemetry_str ON TABLE transformed.telemetry APPEND_ONLY = TRUE;
			END;
        ';
	END IF;
END;
$$