-- ------------------------------------------------------------------------------------------------------------------------------
-- Start dates by site for energy scores
-- ------------------------------------------------------------------------------------------------------------------------------

BEGIN;
	SET default_type = 'EnergyDataStartDate';
	-- DELETE STATEMENT is not supported with cte's; persisting as a table.
	CREATE OR REPLACE TEMPORARY TABLE transformed.cte_site_defaults AS (
	SELECT * FROM (VALUES 
	('f195a533-56a0-45ff-aa27-e58cca387c15', '1 Market Street',    '{"SiteStartDate" : "2022-08-01"}'),
	('5f94942a-aad6-42ea-b0d4-52127f3a5bb3', '120 Collins Street', '{"SiteStartDate" : "2022-09-01"}'),
	('a6b78f54-9875-47bc-9612-aa991cc464f3', '126 Phillip Street', '{"SiteStartDate" : "2022-03-20"}'),
	('952b3038-25c2-44e2-8204-666995d047d1', '135 King Street',    '{"SiteStartDate" : "2022-09-01"}'),
	('e719ac18-192b-4174-91db-b3a624f1f1a4', '151 Clarence Street','{"SiteStartDate" : "2020-01-01"}'),
	('993a3866-d5e4-4239-b2a4-7ce4cb1e4dc9', '201 Kent Street',    '{"SiteStartDate" : "2022-09-01"}'),
	('76ceaffe-c94b-4329-8d40-94249606235d', '259 Queen Street',   '{"SiteStartDate" : "2022-09-01"}'),
	('8e24da1d-3257-46be-af91-81a2e31a4417', '347 Kent Street',    '{"SiteStartDate" : "2022-01-17"}'),
	('f1914666-4050-4ff7-afd7-013bae2eee97', '40 Mount Street',    '{"SiteStartDate" : "2022-01-21"}'),
	('d227f436-a3e1-4775-ac05-6b8b1c121aac', '420 George Street',  '{"SiteStartDate" : "2022-04-06"}'),
	('934638e3-4bd7-4749-bd52-bd6e47d0fbb2', '567 Collins Street', '{"SiteStartDate" : "2020-01-01"}'),
	('404bd33c-a697-4027-b6a6-677e30a53d07', '60 Martin Place',    '{"SiteStartDate" : "2020-01-01"}')
	
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

BEGIN;
	SET default_type = 'AssetStartDate';
	-- DELETE STATEMENT is not supported with cte's; persisting as a table.
	CREATE OR REPLACE TEMPORARY TABLE transformed.cte_site_defaults AS (
	SELECT * FROM (VALUES 
	('e719ac18-192b-4174-91db-b3a624f1f1a4','151 Clarence Street','{"AssetStartDate" : "2023-01-01","AssetId":"INV-151CS-MSB-LGD-01"}')
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