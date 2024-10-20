-- ------------------------------------------------------------------------------------------------------------------------------
-- ReturnAirTemperatureSetpointDefault by site for comfort scores
-- ------------------------------------------------------------------------------------------------------------------------------
BEGIN;
	SET default_type = 'MaxOccupancy';

	CREATE OR REPLACE TEMPORARY TABLE transformed.cte_site_defaults AS (
		SELECT 'e5f6948f-f265-4ca1-b316-50396696a3b2' AS site_id,'200 Liberty Street' AS site_name, 4000 AS max_occupancy
		UNION ALL 
		SELECT '5560037e-6094-4f6d-89f0-6a7d7d06c77f' AS site_id,'225 Liberty Street' AS site_name, 5280 AS max_occupancy
		UNION ALL 
		SELECT '8d2e9886-35d8-47cf-9730-82b981d8f35c' AS site_id,'250 Vesey Street' AS site_name, 3700 AS max_occupancy
		UNION ALL 
		SELECT '351f0da6-e676-4707-8b24-0d17f8e6b777' AS site_id,'1 Liberty Plaza' AS site_name, 8500 AS max_occupancy
		UNION ALL 
		SELECT 'd119b558-575e-45ab-88ef-66bdc3985007' AS site_id,'300 Madison Avenue' AS site_name, 4000 AS max_occupancy
		UNION ALL 
		-- SELECT '8d139f57-840a-4893-b914-5008bcc9bdfc' AS site_id,'660 5th Ave' AS site_name, 0 AS max_occupancy
		-- UNION ALL 
		SELECT 'a604515c-3dc5-487f-a137-bbe54f9c6f51' AS site_id,'4 Manhattan West' AS site_name, 325 AS max_occupancy
		UNION ALL 
		SELECT '4e5fc229-ffd9-462a-882b-16b4a63b2a8a' AS site_id,'1 Manhattan West' AS site_name, 7500 AS max_occupancy
		UNION ALL 
		SELECT 'a7b889e1-0382-4b64-b79a-37fd5c42a1fb' AS site_id,'5 Manhattan West' AS site_name, 3600 AS max_occupancy
		UNION ALL 
		SELECT '24695d9d-269c-4763-966c-b3ab5992dc52' AS site_id,'Grace Building' AS site_name, 5500 AS max_occupancy
		);
	
	DELETE FROM transformed.site_defaults st
	WHERE st.type = $default_type
	  AND st.site_id IN (SELECT site_id from transformed.cte_site_defaults);

	INSERT INTO transformed.site_defaults (site_id, type, default_value, _is_active, _valid_from, _valid_to)
	  SELECT
		s.site_id, 
		$default_type,
		max_occupancy,
		true,
		TO_TIMESTAMP('2000-01-01'), 
		TO_TIMESTAMP('9999-12-31')
	  FROM transformed.cte_site_defaults s;
COMMIT;