-- ******************************************************************************************************************************
-- Create view
-- ******************************************************************************************************************************

CREATE OR REPLACE VIEW transformed.tenant_energy_utility_account_v AS

WITH
    cte_utilityaccount_hour AS (
        SELECT 
			ts.date_local,
			ts.date_time_local_hour,
            ca.asset_id,
			AVG(ts.telemetry_value) AS avg_usage_kwh,
            ca.site_id,
			ca.building_id,
			ca.time_zone,
			ca.capability_id,
			ca.capability_name,
			regexp_substr(asset_id, 'ELEC\\W+(\\w+)', 1, 1, 'e', 1) as acct,
			regexp_substr(asset_id, 'BPY\\W+(\\w+)', 1, 1, 'e', 1) as building_suffix,
            MAX(timestamp_utc) AS last_captured_at_utc,
            MAX(timestamp_local) AS last_captured_at_local,
            MAX(_last_updated_at) AS last_refreshed_at_utc
        FROM transformed.time_series_enriched ts
        JOIN transformed.capabilities_assets ca ON ca.trend_id = ts.trend_id
        WHERE (date_local >= '2022-07-01')
          AND (model_id_asset = 'dtmi:com:willowinc:UtilityAccount;1')
          AND (model_id IN ('dtmi:com:willowinc:BilledActiveElectricalEnergy;1', 'dtmi:com:willowinc:BilledActiveElectricalPower;1'))
        GROUP BY ts.date_local, ts.date_time_local_hour, ca.asset_id, ca.capability_id, ca.capability_name, ca.site_id, ca.building_id, ca.time_zone
    )
	,cte_utility_building_day AS (
        SELECT 
			date_local,
            --asset_id,
			SUM(avg_usage_kwh) AS daily_usage_kwh,
            site_id,
			building_id,
			time_zone,
            MAX(last_captured_at_utc) AS last_captured_at_utc,
            MAX(last_captured_at_local) AS last_captured_at_local,
            MAX(last_refreshed_at_utc) AS last_refreshed_at_utc
        FROM cte_utilityaccount_hour
		WHERE acct = building_suffix
		  AND RIGHT(capability_name,8) = 'BLDPWRIN'
        GROUP BY date_local, site_id, building_id, time_zone --,asset_id
	)
	,cte_utility_tenant_day AS (
        SELECT 
			date_local,
            asset_id,
			SUM(avg_usage_kwh) AS daily_usage_kwh,
            site_id,
			building_id,
			time_zone,
            MAX(last_captured_at_utc) AS last_captured_at_utc,
            MAX(last_captured_at_local) AS last_captured_at_local,
            MAX(last_refreshed_at_utc) AS last_refreshed_at_utc
        FROM cte_utilityaccount_hour
		WHERE acct != building_suffix
        GROUP BY date_local, asset_id, site_id, building_id, time_zone
	)
	,cte_utility_tenant_total AS (
        SELECT 
			date_local,
            --asset_id,
			SUM(daily_usage_kwh) AS daily_usage_kwh,
            site_id,
			building_id,
			time_zone,
            MAX(last_captured_at_utc) AS last_captured_at_utc,
            MAX(last_captured_at_local) AS last_captured_at_local,
            MAX(last_refreshed_at_utc) AS last_refreshed_at_utc
        FROM cte_utility_tenant_day
		GROUP BY date_local, site_id, building_id, time_zone --,asset_id
	)
	,cte_building_tenant_diff AS (
        SELECT 
			b.date_local,
			GREATEST(0, b.daily_usage_kwh - t.daily_usage_kwh) AS daily_usage_kwh,
            b.site_id,
			b.building_id,
			b.time_zone,
            b.last_captured_at_utc,
            b.last_captured_at_local,
            b.last_refreshed_at_utc
        FROM cte_utility_building_day b
		LEFT JOIN cte_utility_tenant_total t
			   ON (b.date_local = t.date_local AND b.building_id = t.building_id)
	)
	,cte_utilityaccount_day AS (
		SELECT
			date_local,
			asset_id,
			daily_usage_kwh,
			site_id,
			building_id,
			time_zone,
			last_captured_at_utc,
			last_captured_at_local,
			last_refreshed_at_utc
		FROM cte_utility_tenant_day

		UNION ALL 

		SELECT
			date_local,
			'Other' AS asset_id,
			daily_usage_kwh,
			site_id,
			building_id,
			time_zone,
			last_captured_at_utc,
			last_captured_at_local,
			last_refreshed_at_utc
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
	)
		SELECT
			ts.date_local,
			DAYOFWEEKISO(ts.date_local) AS day_of_week,
			CASE WHEN day_of_week > 5 THEN false ELSE true END AS is_weekday,
			IFF(is_weekday = TRUE, 'Weekday', 'Weekend') AS day_of_week_type,
			-- NULL AS unified_daily_usage_kwh,
			-- NULL AS unified_1_week_ago_daily_kwh,
			ts.daily_usage_kwh AS power_consumption,
			--NULL AS power_based_daily_usage_kwh,
	    	ts_1_week.daily_usage_kwh AS daily_usage_kwh_1_week_ago,
			--NULL AS power_based_daily_usage_kwh_1_week_ago,
	        ts_4_weeks.daily_usage_kwh AS daily_usage_kwh_4_weeks_ago,
			--NULL AS power_based_usage_kwh_4_weeks_ago,
			CASE WHEN ts.daily_usage_kwh = 0 THEN NULL
				ELSE ( ts.daily_usage_kwh - daily_usage_kwh_1_week_ago ) / ts.daily_usage_kwh
			END AS deviation,
			CASE 
				WHEN deviation IS NULL THEN NULL
				WHEN deviation > 0.15 OR deviation < -0.5 THEN 0
				ELSE 1
			END AS energy_rating,
			CASE WHEN ts.daily_usage_kwh = 0 THEN NULL
				ELSE ( ts.daily_usage_kwh - daily_usage_kwh_4_weeks_ago ) / ts.daily_usage_kwh
			END AS deviation_based_on_4_weeks_ago,
			CASE 
				WHEN deviation_based_on_4_weeks_ago IS NULL THEN NULL
				WHEN deviation_based_on_4_weeks_ago > 0.15 OR deviation_based_on_4_weeks_ago < -0.5 THEN 0
				ELSE 1
			END AS energy_rating_based_on_4_weeks_ago,
			tenant_account_id AS asset_id,
			'N/A' AS asset_name,
			'N/A' AS model_id_asset,
			'N/A' AS floor_id,
			'N/A' AS level_name,
            'N/A' AS floor_sort_order,
			COALESCE(ta.tenant_id,'Other') AS tenant_id,
			COALESCE(ta.tenant_name,'Other') AS tenant_name,
			'N/A' AS tenant_unit_id,
			'N/A' AS tenant_unit_name,
			CASE WHEN ta.tenant_name IS NULL THEN GREATEST(0, b.rentable_area - bt.rentable_area) ELSE tl.rentable_area END AS tenant_unit_rentable_area,
			ts.site_id,
			b.site_name,
			b.customer_id,
			b.portfolio_id,
			tl.building_id,
			tl.building_name,
			COALESCE(b.gross_area,b.rentable_area) AS building_gross_area,
			COALESCE(b.rentable_area,b.gross_area) AS building_rentable_area,
			b.type AS building_type,
			-- NULL AS end_of_day_kwh,
			-- NULL AS end_of_prev_day_value_kwh,
			-- NULL AS daily_usage_kwh_eod_calc,
			ts.last_captured_at_local,
			ts.last_captured_at_utc,
			ts.last_refreshed_at_utc,
			CONVERT_TIMEZONE( 'UTC',ts.time_zone, ts.last_refreshed_at_utc) AS last_refreshed_at_local
		FROM cte_utilityaccount_day ts
        LEFT JOIN cte_utilityaccount_day ts_1_week
			   ON (ts.asset_id =  ts_1_week.asset_id AND ts.building_id =  ts_1_week.building_id AND DATEADD('week',-1,ts.date_local) = ts_1_week.date_local)
        LEFT JOIN cte_utilityaccount_day ts_4_weeks
			   ON (ts.asset_id = ts_4_weeks.asset_id AND ts.building_id =  ts_4_weeks.building_id AND DATEADD('week',-4,ts.date_local) = ts_4_weeks.date_local)	
		LEFT JOIN cte_tenant_account ta
               ON (ts.asset_id = ta.tenant_account_id) AND ts.site_id =  ta.site_id
        LEFT JOIN cte_tenant_lease tl
			   ON (ta.tenant_id = tl.tenant_id)
			 AND (ts.building_id = tl.building_id)
			 -- AND (ts.date_local BETWEEN tl.lease_start AND tl.lease_end)
		LEFT JOIN transformed.buildings b 
		       ON (ts.building_id = b.building_id)
		JOIN cte_building_tenants bt ON b.building_id = bt.building_id
        ;
CREATE OR REPLACE TABLE transformed.tenant_energy_utility_account AS SELECT * FROM transformed.tenant_energy_utility_account_v;