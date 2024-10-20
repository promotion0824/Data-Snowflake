-- ******************************************************************************************************************************
-- Stored procedure to clone data from prd to {env}
-- ******************************************************************************************************************************
EXECUTE IMMEDIATE
$$
DECLARE 
	currentdb STRING;
BEGIN
	SELECT CURRENT_DATABASE() INTO :currentdb From dual;
	IF (currentdb='UAT_DB') THEN
		CREATE OR REPLACE PROCEDURE raw.clone_from_prd_raw_sp()
            RETURNS STRING
			LANGUAGE SQL
		AS
        '
			BEGIN
				CREATE OR REPLACE TABLE raw.stage_ontology CLONE PRD_DB.raw.stage_ontology;
			END;
        ';
	END IF;
END;
$$
