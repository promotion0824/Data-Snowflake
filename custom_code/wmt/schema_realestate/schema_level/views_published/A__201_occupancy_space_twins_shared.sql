-- ******************************************************************************************************************************
-- Create view and persist as a table in transformed
-- ******************************************************************************************************************************CREATE OR REPLACE VIEW transformed.space_occupancy_assets AS 
CREATE OR REPLACE SECURE VIEW published.occupancy_space_twins_shared AS
SELECT *
FROM published.occupancy_space_twins
;

EXECUTE IMMEDIATE
$$
DECLARE 
	currentdb STRING;
BEGIN
	SELECT CURRENT_DATABASE() INTO :currentdb From dual;
	IF (currentdb='PRD_DB') THEN
		GRANT SELECT ON VIEW published.occupancy_space_twins_shared TO SHARE external_share;
	END IF;
END;
$$