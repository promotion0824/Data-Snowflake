-- ------------------------------------------------------------------------------------------------------------------------------
-- ReturnAirTemperatureSetpointDefault by site for comfort scores
-- ------------------------------------------------------------------------------------------------------------------------------
BEGIN;
	SET default_type = 'ReturnAirTemperatureSetpointDefault';

	CREATE OR REPLACE TEMPORARY TABLE transformed.cte_site_defaults AS (
		SELECT site_id,name as site_name, '{"unit": "degF","value": 72}' AS default_setpoint
		FROM transformed.directory_core_sites 
		);
	
	DELETE FROM transformed.site_defaults st
	WHERE st.type = $default_type
	  AND st.site_id IN (SELECT site_id from transformed.cte_site_defaults);

	INSERT INTO transformed.site_defaults (site_id, type, default_value, _is_active, _valid_from, _valid_to)
	  SELECT
		s.site_id, 
		$default_type,
		TRY_PARSE_JSON((default_setpoint)::variant),
		true,
		TO_TIMESTAMP('2000-01-01'), 
		TO_TIMESTAMP('9999-12-31')
	  FROM transformed.cte_site_defaults s;
COMMIT;