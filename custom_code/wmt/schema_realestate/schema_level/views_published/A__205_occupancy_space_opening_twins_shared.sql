-- ------------------------------------------------------------------------------------------------------------------------------
-- create View
-- ------------------------------------------------------------------------------------------------------------------------------CREATE OR REPLACE VIEW published.occupancy_space_opening_twins AS 
CREATE OR REPLACE SECURE VIEW published.occupancy_space_opening_twins_shared AS 
SELECT *
FROM published.occupancy_space_opening_twins 
;

EXECUTE IMMEDIATE
$$
DECLARE 
	currentdb STRING;
BEGIN
	SELECT CURRENT_DATABASE() INTO :currentdb From dual;
	IF (currentdb='PRD_DB') THEN
		GRANT SELECT ON VIEW published.occupancy_space_opening_twins_shared TO SHARE external_share;
	END IF;
END;
$$