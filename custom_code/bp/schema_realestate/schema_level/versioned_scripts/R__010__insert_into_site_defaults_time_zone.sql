-- ------------------------------------------------------------------------------------------------------------------------------
-- Start dates by site for energy scores
-- These need to use the converted values in the function: utils.convert_time_zone_name_from_windows_to_tzdata_udf
-- ------------------------------------------------------------------------------------------------------------------------------

BEGIN;
	SET default_type = 'DefaultTimeZone';
	-- DELETE STATEMENT is not supported with cte's; persisting as a table.
	CREATE OR REPLACE TEMPORARY TABLE transformed.cte_site_defaults AS (
	SELECT * FROM (VALUES 
	('', '',    '{"DefaultTimeZone" : "America/New_York"}')

	) AS s (site_id,site_name,default_time_zone)
	);
	DELETE FROM transformed.site_defaults st
	WHERE st.type = $default_type
	  AND st.site_id IN (SELECT site_id from transformed.cte_site_defaults);

	INSERT INTO transformed.site_defaults (site_id, type, default_value, _is_active, _valid_from, _valid_to)
	  SELECT
		s.site_id, 
		$default_type,
		TRY_PARSE_JSON((default_time_zone)::variant),
		true,
		TO_TIMESTAMP('2000-01-01'), 
		TO_TIMESTAMP('9999-12-31')
	  FROM transformed.cte_site_defaults s;
COMMIT;
