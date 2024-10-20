-- ------------------------------------------------------------------------------------------------------------------------------
-- Start dates by site for energy scores
-- ------------------------------------------------------------------------------------------------------------------------------
BEGIN;
	SET default_type = 'EnergyMinThreshold';
	-- DELETE STATEMENT is not supported with cte's; persisting as a table.
	CREATE OR REPLACE TEMPORARY TABLE transformed.cte_site_defaults AS (
	SELECT * FROM (VALUES 
	('e57539e0-d938-400a-b6b4-e9cb21dcc535', 'Stories',    '{"EnergyMinThreshold" : 0}')
	) AS s (site_id,site_name,energy_min_threshold)
	);
	DELETE FROM transformed.site_defaults st
	WHERE st.type = $default_type
	  AND st.site_id IN (SELECT site_id from transformed.cte_site_defaults);

	INSERT INTO transformed.site_defaults (site_id, type, default_value, _is_active, _valid_from, _valid_to)
	  SELECT
		s.site_id, 
		$default_type,
		TRY_PARSE_JSON((energy_min_threshold)::variant),
		true,
		TO_TIMESTAMP('2000-01-01'), 
		TO_TIMESTAMP('9999-12-31')
	  FROM transformed.cte_site_defaults s;
COMMIT;

BEGIN;
	SET default_type = 'AssetStartDate';
	-- DELETE STATEMENT is not supported with cte's; persisting as a table.
	CREATE OR REPLACE TEMPORARY TABLE transformed.cte_site_defaults AS (
	SELECT * FROM (VALUES 
	('e57539e0-d938-400a-b6b4-e9cb21dcc535','Stories','{"AssetStartDate" : "2022-01-18","AssetId":"AXA-STO-TGS_A_RCB_PK_A"}')
	) AS s (site_id,site_name,energy_start_date)
	);
	DELETE FROM transformed.site_defaults st
	WHERE st.type = $default_type
	  AND st.site_id IN (SELECT site_id from transformed.cte_site_defaults);

	INSERT INTO transformed.site_defaults (site_id, type, default_value, _is_active, _valid_from, _valid_to)
	  SELECT
		s.site_id, 
		$default_type,
		TRY_PARSE_JSON((energy_start_date)::variant),
		true,
		TO_TIMESTAMP('2000-01-01'), 
		TO_TIMESTAMP('9999-12-31')
	  FROM transformed.cte_site_defaults s;
COMMIT;
