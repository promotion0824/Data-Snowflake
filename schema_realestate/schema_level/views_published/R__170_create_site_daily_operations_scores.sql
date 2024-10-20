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
            WHERE (date >= '2022-01-01')
            AND date <= DATEADD(d,1,current_date())
        )
        ,cte_comfort AS (
        SELECT
            site_id,
            date,
            is_working_hour,
            AVG(comfort_score) AS comfort_score,
            IFNULL(excludedFromComfortAnalytics,false) AS excludedFromComfortAnalytics,
            MAX(last_captured_at_local) AS last_captured_at_local,
            MAX(SYSDATE()) AS last_refreshed_at_utc
        FROM published.comfort_daily_metrics
        WHERE IFNULL(excludedFromComfortAnalytics,false) = false
        GROUP BY building_id, site_id, date,is_weekday,is_working_hour,IFNULL(excludedFromComfortAnalytics,false)
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
        ,cte_overall AS (
        SELECT 
            dates.building_id,
            dates.building_name,
            dates.site_id, 
            dates.site_name, 
            dates.date, 
            dates.is_weekday,
            dates.is_working_hour,
            comfort.excludedFromComfortAnalytics,
            ROUND(comfort.comfort_score, 10) AS comfort_score, 
            ROUND(energy.energy_score, 10) AS energy_score,
            IFNULL(comfort.comfort_score,0) + IFNULL(energy.energy_score,0) AS combined_score,
            CASE WHEN comfort.comfort_score IS NULL THEN 0 ELSE 1 END +
            CASE WHEN energy.energy_score IS NULL THEN 0 ELSE 1 END AS number_of_scores,
            CASE WHEN number_of_scores = 0 THEN NULL ELSE ROUND(combined_score / number_of_scores, 10) END AS overall_score,
            GREATEST(comfort.last_captured_at_local,energy.last_captured_at_local) AS last_captured_at_local, 
            GREATEST(energy.last_captured_at_utc) AS last_captured_at_utc,
            GREATEST(comfort.last_refreshed_at_utc,energy.last_refreshed_at_utc) AS last_refreshed_at_utc,
            CONVERT_TIMEZONE( 'UTC',dates.time_zone, GREATEST(comfort.last_refreshed_at_utc,energy.last_refreshed_at_utc)) AS last_refreshed_at_local
        FROM cte_sites_dates dates
            LEFT JOIN cte_comfort comfort 
                ON (dates.site_id = comfort.site_id)
               AND (dates.date = comfort.date)
               AND (dates.is_working_hour = comfort.is_working_hour)
            LEFT JOIN cte_energy energy 
                ON (dates.site_id = energy.site_id)
               AND (dates.date = energy.date)
               AND (dates.is_working_hour = energy.is_working_hour)
            LEFT JOIN transformed.site_defaults default_setpoints
            ON (
                    dates.site_id = default_setpoints.site_id 
                AND default_setpoints.type = 'ComfortDataStartDate' 
                AND default_setpoints._valid_from <= comfort.date 
                AND default_setpoints._valid_to >= comfort.date
            )
        WHERE (comfort.date >= IFNULL(default_setpoints.default_value:SiteStartDate,'2019-01-01')
        OR comfort.date IS NULL)
        )
        SELECT
            overall.building_id,
            overall.building_name,
            overall.site_id,
            overall.site_name,
            overall.date, 
            overall.is_weekday,
            overall.is_working_hour,
            overall.excludedFromComfortAnalytics,
            overall.comfort_score,
            overall.energy_score,
            overall.overall_score,
            overall_month_ago.comfort_score AS comfort_score_1_month_ago,
            overall_month_ago.energy_score AS energy_score_1_month_ago,
            overall_month_ago.overall_score AS overall_score_1_month_ago,
            overall_year_ago.comfort_score AS comfort_score_1_year_ago, 
            overall_year_ago.energy_score AS energy_score_1_year_ago,
            overall_year_ago.overall_score AS overall_score_1_year_ago,
            overall.last_captured_at_local,
            overall.last_captured_at_utc,
            overall.last_refreshed_at_utc,
            overall.last_refreshed_at_local
        FROM cte_overall overall
        LEFT JOIN cte_overall overall_month_ago
              ON (overall.site_id = overall_month_ago.site_id)
              AND (DATEADD('month',-1,overall.date) = overall_month_ago.date)
              AND (overall.is_working_hour = overall_month_ago.is_working_hour)
        LEFT JOIN cte_overall overall_year_ago
               ON (overall.site_id = overall_year_ago.site_id)
              AND (DATEADD('year',-1,overall.date) = overall_year_ago.date)
              AND (overall.is_working_hour = overall_year_ago.is_working_hour)
;