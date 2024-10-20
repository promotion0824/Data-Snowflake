-- ******************************************************************************************************************************
-- Create site_daily_operations_scores
-- ******************************************************************************************************************************
CREATE OR REPLACE VIEW published.site_daily_operations_scores AS
    WITH cte_sites_dates AS (
    SELECT DISTINCT
                s.site_id,
                s.name AS site_name,
                s.building_id,
                s.building_name,
                date,
                is_weekday, 
                IFF(HOUR(date_time_hour) >= COALESCE(working_hours.default_value:hourStart,8) 
                    AND HOUR(date_time_hour) <  COALESCE(working_hours.default_value:hourEnd, 18), true, false) AS is_working_hour,
                s.time_zone
            FROM transformed.date_hour d
            CROSS JOIN transformed.sites s
            LEFT JOIN transformed.site_defaults working_hours
                ON (s.site_id = working_hours.site_id AND working_hours.type = 'WorkingHours')
                AND (working_hours._valid_from <= d.date AND working_hours._valid_to >= d.date)
            WHERE (date >= '2022-05-20')
            AND date <= DATEADD(d,1,current_date())
        )
        ,cte_comfort AS (
    -- original from zone air calcs
        SELECT 
            site_id, 
            date, 
            is_working_hour,
            ROUND(comfort.comfort_score, 10) AS comfort_score,
            excludedFromComfortAnalytics,
            last_captured_at_local, 
            last_refreshed_at_utc
        FROM transformed.agg_site_daily_comfort_scores comfort 
        WHERE IFNULL(Excludedfromcomfortanalytics,false) = false
        AND (SELECT count(*) FROM published.comfort_daily_metrics) = 0

    UNION 

    -- from new comfort calcs
        SELECT
            site_id,
            date,
            is_working_hour,
            AVG(comfort_score) AS comfort_score,
            excludedFromComfortAnalytics,
            MAX(last_captured_at_local) AS last_captured_at_local,
            MAX(SYSDATE()) AS last_refreshed_at_utc
        FROM published.comfort_daily_metrics
        GROUP BY building_id, site_id, date,is_weekday,is_working_hour,excludedFromComfortAnalytics
        )
        ,cte_energy AS (
        -- energy is calculated at the daily level; can't really break it up by hour; so just union the two working hour values
        SELECT 
            energy.site_id, 
            s.building_id,
            s.building_name,
            date, 
            'TRUE'::BOOLEAN AS is_working_hour,
            ROUND(energy.energy_score, 10) AS energy_score, 
            last_captured_at_local, 
            last_captured_at_utc,
            last_refreshed_at_utc
        FROM transformed.site_daily_scores energy
        JOIN transformed.sites s on energy.site_id = s.site_id

        UNION ALL

        SELECT 
            energy.site_id,
            s.building_id,
            s.building_name,
            date, 
            'FALSE'::BOOLEAN AS is_working_hour,
            ROUND(energy.energy_score, 10) AS energy_score, 
            last_captured_at_local, 
            last_captured_at_utc,
            last_refreshed_at_utc
    FROM transformed.site_daily_scores energy 
    JOIN transformed.sites s on energy.site_id = s.site_id
    )
        SELECT
            dates.building_id,
            dates.building_name,
            dates.site_id, 
            dates.site_name, 
            dates.date, 
            dates.is_weekday,
            dates.is_working_hour,
            comfort.excludedFromComfortAnalytics, 
            CASE WHEN comfort.comfort_score IS NULL THEN uniform(50, 100, random()) ELSE ROUND(comfort.comfort_score, 10) END AS comfort_score_calc, 
            CASE WHEN energy.energy_score IS NULL THEN uniform(50, 100, random()) ELSE ROUND(energy.energy_score, 10) END AS energy_score_calc,
            comfort_score_calc AS comfort_score,
            energy_score_calc AS energy_score,
            ROUND((comfort_score_calc + energy_score_calc) / 2, 10) AS overall_score,
            ROUND(comfort_month_ago.comfort_score,10) AS comfort_score_1_month_ago,
            ROUND(energy_month_ago.energy_score,10) AS energy_score_1_month_ago,
            ROUND((comfort_month_ago.comfort_score + energy_month_ago.energy_score) / 2, 10) AS overall_score_1_month_ago,
            ROUND(comfort_year_ago.comfort_score,10) AS comfort_score_1_year_ago,
            ROUND(energy_year_ago.energy_score,10) AS energy_score_1_year_ago,
            ROUND((comfort_year_ago.comfort_score + energy_year_ago.energy_score) / 2, 10) AS overall_score_1_year_ago,
            GREATEST(comfort.last_captured_at_local,energy.last_captured_at_local) AS last_captured_at_local, 
            GREATEST(energy.last_captured_at_utc) AS last_captured_at_utc,
            GREATEST(comfort.last_refreshed_at_utc,energy.last_refreshed_at_utc) AS last_refreshed_at_utc,
            CONVERT_TIMEZONE( 'UTC',dates.time_zone, GREATEST(comfort.last_refreshed_at_utc,energy.last_refreshed_at_utc)) AS last_refreshed_at_local
        FROM cte_sites_dates dates
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
            LEFT JOIN cte_energy energy 
                ON (dates.site_id = energy.site_id)
               AND (dates.date = energy.date)
               AND (dates.is_working_hour = energy.is_working_hour)
            LEFT JOIN cte_energy energy_month_ago
                ON (dates.site_id = energy_month_ago.site_id)
               AND (DATEADD('month',-1,dates.date) = energy_month_ago.date)
               AND (dates.is_working_hour = energy_month_ago.is_working_hour)
            LEFT JOIN cte_energy energy_year_ago
                ON (dates.site_id = energy_year_ago.site_id)
               AND (DATEADD('year',-1,energy.date) = energy_year_ago.date)
               AND (dates.is_working_hour = energy_year_ago.is_working_hour)
            LEFT JOIN transformed.site_defaults default_setpoints
            ON (
                    dates.site_id = default_setpoints.site_id 
                AND default_setpoints.type = 'ComfortDataStartDate' 
                AND default_setpoints._valid_from <= comfort.date 
                AND default_setpoints._valid_to >= comfort.date
            )
        WHERE (comfort.date >= IFNULL(default_setpoints.default_value:SiteStartDate,'2019-01-01')
        OR comfort.date IS NULL)
    ;