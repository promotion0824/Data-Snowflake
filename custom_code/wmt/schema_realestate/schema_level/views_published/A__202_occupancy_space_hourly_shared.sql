-- ------------------------------------------------------------------------------------------------------------------------------
-- create View
-- ------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE SECURE VIEW published.occupancy_space_hourly_shared AS
SELECT *
FROM published.occupancy_space_hourly;

EXECUTE IMMEDIATE
$$
DECLARE 
	currentdb STRING;
BEGIN
	SELECT CURRENT_DATABASE() INTO :currentdb From dual;
	IF (currentdb='PRD_DB') THEN
		GRANT SELECT ON VIEW published.occupancy_space_hourly_shared TO SHARE external_share;
	END IF;
END;
$$