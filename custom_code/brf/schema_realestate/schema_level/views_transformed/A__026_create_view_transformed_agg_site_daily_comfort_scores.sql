-- ******************************************************************************************************************************
-- Create view of comfort scores aggregated by site and day
-- This is in published folder because to facilitate deployment; it depends on a published view
-- ******************************************************************************************************************************

CREATE OR REPLACE VIEW transformed.agg_site_daily_comfort_scores_v AS
	WITH cte_daily_agg AS (
	  SELECT 
		site_id, 
		date,
		CASE WHEN day_of_week_type = 'Weekday' THEN 1 ELSE 0 END AS is_weekday,
		is_working_hour,
		excludedFromComfortAnalytics,
		AVG(return_air_temperature_score) AS comfort_score,
		MAX(last_captured_at_local) AS last_captured_at_local,
		MAX(last_captured_at_utc) AS last_captured_at_utc,
		MAX(last_refreshed_at_utc) AS last_refreshed_at_utc
	  FROM transformed.return_air_temperature_daily_metrics
	  GROUP BY site_id, date, is_weekday, is_working_hour,excludedFromComfortAnalytics
	)
		SELECT 
			cte.site_id, 
			cte.date,
			cte.is_weekday,
			cte.is_working_hour,
			cte.excludedFromComfortAnalytics,
			cte.comfort_score,
			LAG(cte.comfort_score, 7, 0) OVER (PARTITION BY cte.site_id ORDER BY cte.site_id, cte.date) AS comfort_score_1_week_ago,
            cte1.comfort_score AS comfort_score_1_month_ago,
            cte.last_captured_at_local,
            cte.last_captured_at_utc,
            cte.last_refreshed_at_utc
		FROM cte_daily_agg cte
        LEFT JOIN cte_daily_agg cte1
               ON (cte1.site_id = cte.site_id)
              AND (cte1.date = DATEADD('month',-1,cte.date))
			  AND (cte1.is_working_hour = cte.is_working_hour)
;


CREATE OR REPLACE TABLE transformed.agg_site_daily_comfort_scores AS SELECT * FROM transformed.agg_site_daily_comfort_scores_v;