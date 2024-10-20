-- ******************************************************************************************************************************
-- Stored procedure to CREATE TABLE transformed.tenant_energy_utility_account
-- ******************************************************************************************************************************
CREATE OR REPLACE PROCEDURE transformed.merge_tenant_energy_utility_account_sp() 
    RETURNS STRING
    LANGUAGE SQL
  AS
    $$
	BEGIN
		CREATE OR REPLACE TABLE transformed.utility_account_power_consumption AS
			SELECT
				ts.date_local,
				ts.date_time_local_hour,
				ts.date_time_local_15min,
				ca.asset_id,
				SUM(ts.telemetry_value) AS power_consumption,
				ca.site_id,
				ca.building_id,
				regexp_substr(ca.asset_id, 'ELEC\\W+(\\w+)', 1, 1, 'e', 1) as acct,
				SPLIT_PART(ca.asset_id, '-', 2) as building_suffix,
				MAX(timestamp_utc) AS last_captured_at_utc,
				MAX(timestamp_local) AS last_captured_at_local,
				MAX(_last_updated_at) AS last_refreshed_at_utc,
				MAX(CONVERT_TIMEZONE( 'UTC',ca.time_zone, ts._last_updated_at)) AS last_refreshed_at_local
			FROM transformed.time_series_enriched ts
			JOIN transformed.capabilities_assets ca ON ca.trend_id = ts.trend_id
			WHERE (date_local >= '2022-12-01')
			AND (date_local <= DATEADD('day',-1,CURRENT_DATE()))
			AND (model_id_asset = 'dtmi:com:willowinc:UtilityAccount;1')
			AND (model_id IN ('dtmi:com:willowinc:BilledActiveElectricalPower;1','dtmi:com:willowinc:ActiveElectricalPowerSensor;1'))
			AND ca.capability_id NOT LIKE '%PWROUT%'
			AND ca.capability_id NOT LIKE '%-SUPP-%'
		GROUP BY ts.date_local, ts.date_time_local_hour, ts.date_time_local_15min, ca.asset_id, ca.site_id, ca.building_id
		;

		CREATE OR REPLACE TABLE transformed.tenant_energy_utility_account AS 
		WITH cte_utility_tenant_raw AS (
			SELECT 
				date_local,
				date_time_local_hour,
				date_time_local_15min,
				asset_id,
				power_consumption,
				site_id,
				building_id,
				last_captured_at_utc,
				last_captured_at_local,
				last_refreshed_at_utc,
				last_refreshed_at_local
			FROM transformed.utility_account_power_consumption 
			WHERE acct != building_suffix
		)
		,cte_utility_building_total AS (
			SELECT 
				date_local,
				date_time_local_hour,
				date_time_local_15min,
				SUM(power_consumption) AS power_consumption,
				site_id,
				building_id
			FROM transformed.utility_account_power_consumption 
			WHERE acct = building_suffix
			GROUP BY date_local, date_time_local_hour, date_time_local_15min, site_id, building_id
		)
		,cte_utility_tenant_total AS (
			SELECT 
				date_local,
				date_time_local_hour,
				date_time_local_15min,
				SUM(power_consumption) AS power_consumption,
				site_id,
				building_id
			FROM transformed.utility_account_power_consumption 
e			WHERE acct != building_suffix
			GROUP BY date_local, date_time_local_hour, date_time_local_15min, site_id, building_id
		)
		,cte_building_tenant_diff AS (
			SELECT 
				b.date_local,
				b.date_time_local_hour,
				b.date_time_local_15min,
				GREATEST(0, b.power_consumption - t.power_consumption) AS power_consumption,
				b.site_id,
				b.building_id
			FROM cte_utility_building_total b
			LEFT JOIN cte_utility_tenant_total t
				ON (b.date_time_local_15min = t.date_time_local_15min AND b.building_id = t.building_id)
		)
		,cte_utilityaccount_detail AS (
			SELECT
				date_local,
				date_time_local_hour,
				date_time_local_15min,
				asset_id,
				power_consumption,
				site_id,
				building_id,
				last_captured_at_utc,
				last_captured_at_local,
				last_refreshed_at_utc,
				last_refreshed_at_local
			FROM cte_utility_tenant_raw

			UNION ALL 

			SELECT
				date_local,
				date_time_local_hour,
				date_time_local_15min,
				'Other' AS asset_id,
				power_consumption,
				site_id,
				building_id,
				NULL,
				NULL,
				NULL,
				NULL
			FROM cte_building_tenant_diff		
		)
		,cte_building_tenants AS (
		SELECT 
				site_name,
				building_id,
				building_name,
				SUM(rentable_area) as rentable_area
			FROM transformed.tenant_lease
			GROUP BY site_name,building_id, building_name
	)
		,cte_tenant_lease AS (
			SELECT 
				tenant_name,
				tenant_id,
				SUM(rentable_area) as rentable_area,
				-- COALESCE(lease_start, '2000-01-01') AS lease_start,
				-- COALESCE(lease_end,   '2999-01-01') AS lease_end,
				site_name,
				building_id,
				building_name
			FROM transformed.tenant_lease
			GROUP BY tenant_name,tenant_id,site_name,building_id, building_name --,lease_start,lease_end
		)
		,cte_tenant_account AS (
			SELECT DISTINCT
				tr.source_twin_id AS tenant_account_id,
				tr.relationship_name,
				tt.twin_id AS tenant_id,
				tt.name AS tenant_name,
				t.site_id
			FROM transformed.twins t
			JOIN transformed.twins_relationships tr ON t.twin_id = tr.source_twin_id
			JOIN transformed.twins tt ON tr.target_twin_id = tt.twin_id
			WHERE (tr.relationship_name IN ('isHeldBy'))
			AND (tt.model_id = 'dtmi:com:willowinc:Company;1')
			AND IFNULL(t.is_deleted, false) = false
			AND IFNULL(tt.is_deleted, false) = false
			AND IFNULL(tr.is_deleted, false) = false
		)
			SELECT
				ts.date_local,
				ts.date_time_local_hour,
				ts.date_time_local_15min,
				DAYOFWEEKISO(ts.date_local) AS day_of_week,
				CASE WHEN day_of_week > 5 THEN false ELSE true END AS is_weekday,
				IFF(is_weekday = TRUE, 'Weekday', 'Weekend') AS day_of_week_type,
				ts.power_consumption,
				ta.tenant_account_id AS asset_id,
				ta.tenant_account_id AS asset_name,
				COALESCE(ta.tenant_id,'Other') AS tenant_id,
				COALESCE(ta.tenant_name,'Other') AS tenant_name,
				'N/A' AS tenant_unit_id,
				'N/A' AS tenant_unit_name,
				CASE WHEN ta.tenant_name IS NULL THEN GREATEST(0, b.rentable_area - bt.rentable_area) ELSE tl.rentable_area END AS tenant_unit_rentable_area,
				ts.site_id,
				b.site_name,
				b.customer_id,
				b.portfolio_id,
				ts.building_id,
				tl.building_name,
				COALESCE(b.gross_area,b.rentable_area) AS building_gross_area,
				COALESCE(b.rentable_area,b.gross_area) AS building_rentable_area,
				b.type AS building_type,
				ts.last_captured_at_local,
				ts.last_captured_at_utc,
				ts.last_refreshed_at_utc,
				ts.last_refreshed_at_local
			FROM cte_utilityaccount_detail ts
				-- AND (ts.date_local BETWEEN tl.lease_start AND tl.lease_end)
			LEFT JOIN cte_tenant_account ta
				ON (ts.asset_id = ta.tenant_account_id) AND ts.site_id =  ta.site_id
			LEFT JOIN cte_tenant_lease tl
				ON (ta.tenant_id = tl.tenant_id)
				AND (ts.building_id = tl.building_id)
			LEFT JOIN transformed.buildings b 
				ON (ts.building_id = b.building_id)
			LEFT JOIN cte_building_tenants bt
			    ON (b.building_id = bt.building_id)
			;
      END;		
    $$
;