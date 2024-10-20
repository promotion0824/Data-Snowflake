-- ******************************************************************************************************************************
-- Create kpi_building_daily_scores
-- ******************************************************************************************************************************
CREATE OR REPLACE VIEW published.kpi_building_daily_scores AS
	WITH cte_buildings_dates AS (
    SELECT DISTINCT
            s.site_id,
            s.building_id,
            s.building_name,
            date,
            is_weekday, 
            IFF(HOUR(date_time_hour) >= COALESCE(working_hours.default_value:hourStart,8) 
                AND HOUR(date_time_hour) <  COALESCE(working_hours.default_value:hourEnd, 18), true, false) AS is_working_hour,
            s.time_zone
		FROM transformed.date_hour d
        CROSS JOIN transformed.buildings s
        LEFT JOIN transformed.site_defaults working_hours
            ON (s.site_id = working_hours.site_id AND working_hours.type = 'WorkingHours')
            AND (working_hours._valid_from <= d.date AND working_hours._valid_to >= d.date)
        WHERE (date >= IFNULL((SELECT MIN(date) FROM transformed.agg_site_daily_comfort_scores WHERE comfort_score IS NOT NULL),'2023-01-01')
		   OR  date >= IFNULL((SELECT MIN(date) FROM transformed.site_daily_scores WHERE comfort_score IS NOT NULL),'2099-01-01') )
          AND date <= DATEADD(d,1,current_date())
	)
    ,cte_comfort AS (
    SELECT 
        site_id, 
        date, 
        is_working_hour,
        ROUND(comfort.comfort_score, 10) AS comfort_score,
        excludedFromComfortAnalytics,
		last_captured_at_local, 
		last_refreshed_at_utc
    FROM published.comfort_building_daily_scores comfort 
    WHERE IFNULL(Excludedfromcomfortanalytics,false) = false
    )
--     ,cte_energy AS (
--     -- energy is calculated at the daily level; can't really break it up by hour; so just union the two working hour values
--     SELECT 
--         energy.site_id, 
--         s.building_id,
--         s.building_name,
--         date, 
--         'TRUE'::BOOLEAN AS is_working_hour,
--         ROUND(energy.energy_score, 10) AS energy_score, 
-- 		last_captured_at_local, 
-- 		last_captured_at_utc,
-- 		last_refreshed_at_utc
--     FROM transformed.site_daily_scores energy
--     JOIN transformed.sites s on energy.site_id = s.site_id

--     UNION ALL

--     SELECT 
--         energy.site_id,
--         s.building_id,
--         s.building_name,
--         date, 
--         'FALSE'::BOOLEAN AS is_working_hour,
--         ROUND(energy.energy_score, 10) AS energy_score, 
-- 		last_captured_at_local, 
-- 		last_captured_at_utc,
-- 		last_refreshed_at_utc
-- FROM transformed.site_daily_scores energy 
-- JOIN transformed.sites s on energy.site_id = s.site_id
-- )
    SELECT
        dates.building_id,
        dates.building_name,
        dates.site_id, 
        dates.date, 
        dates.is_weekday,
        dates.is_working_hour,
        comfort.excludedFromComfortAnalytics,
        ROUND(comfort.comfort_score, 10) AS comfort_score, 
       -- ROUND(energy.energy_score, 10) AS energy_score,
        --ROUND((comfort.comfort_score + energy.energy_score) / 2, 10) AS overall_score,
        ROUND(comfort_month_ago.comfort_score,10) AS comfort_score_1_month_ago,
		--ROUND(energy_month_ago.energy_score,10) AS energy_score_1_month_ago,
		--ROUND((comfort_month_ago.comfort_score + energy_month_ago.energy_score) / 2, 10) AS overall_score_1_month_ago,
        ROUND(comfort_year_ago.comfort_score,10) AS comfort_score_1_year_ago
		--ROUND(energy_year_ago.energy_score,10) AS energy_score_1_year_ago,
		-- ROUND((comfort_year_ago.comfort_score + energy_year_ago.energy_score) / 2, 10) AS overall_score_1_year_ago,
		-- GREATEST(comfort.last_captured_at_local,energy.last_captured_at_local) AS last_captured_at_local, 
		-- GREATEST(comfort.last_captured_at_utc,energy.last_captured_at_utc) AS last_captured_at_utc,
		-- GREATEST(comfort.last_refreshed_at_utc,energy.last_refreshed_at_utc) AS last_refreshed_at_utc,
        -- CONVERT_TIMEZONE( 'UTC',dates.time_zone, GREATEST(comfort.last_refreshed_at_utc,energy.last_refreshed_at_utc)) AS last_refreshed_at_local
    FROM cte_buildings_dates dates
        -- comfort
        LEFT JOIN cte_comfort comfort 
            ON (dates.site_id = comfort.site_id)
           AND (dates.date = comfort.date)
           AND (dates.is_working_hour = comfort.is_working_hour)
        LEFT JOIN cte_comfort comfort_month_ago
            ON (dates.site_id = comfort_month_ago.site_id)
		   AND (DATEADD('month',-1,dates.date) = comfort_month_ago.date)
           AND (dates.is_working_hour = comfort_month_ago.is_working_hour)
        LEFT JOIN cte_comfort comfort_year_ago
            ON (dates.site_id = comfort_year_ago.site_id)
		   AND (DATEADD('year',-1,dates.date) = comfort_year_ago.date)
           AND (dates.is_working_hour = comfort_year_ago.is_working_hour)
        -- energy        
        -- LEFT JOIN cte_energy energy 
        --     ON (dates.site_id = energy.site_id)
		--    AND (dates.date = energy.date)
        --    AND (dates.is_working_hour = energy.is_working_hour)
        -- LEFT JOIN cte_energy energy_month_ago
        --     ON (dates.site_id = energy_month_ago.site_id)
		--    AND (DATEADD('month',-1,dates.date) = energy_month_ago.date)
        --    AND (dates.is_working_hour = energy_month_ago.is_working_hour)
        -- LEFT JOIN cte_energy energy_year_ago
        --     ON (dates.site_id = energy_year_ago.site_id)
		--    AND (DATEADD('year',-1,energy.date) = energy_year_ago.date)
        --    AND (dates.is_working_hour = energy_year_ago.is_working_hour)
;