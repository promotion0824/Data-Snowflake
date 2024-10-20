-- ******************************************************************************************************************************
-- Create view of comfort scores aggregated by site and day
-- ******************************************************************************************************************************
CREATE OR REPLACE VIEW published.comfort_building_daily_scores AS
	WITH cte_daily_agg AS (
	  SELECT 
		building_id,
		site_id,
		date,
		is_weekday,
		is_working_hour,
		excludedFromComfortAnalytics,
		AVG(comfort_score) AS comfort_score,
		MAX(last_captured_at_local) AS last_captured_at_local,
		MAX(SYSDATE()) AS last_refreshed_at_utc
	  FROM published.comfort_daily_metrics
	  GROUP BY building_id, site_id, date,is_weekday,is_working_hour,excludedFromComfortAnalytics
	)
		SELECT 
			cte.building_id, 
			cte.site_id,
			cte.date,
			cte.is_weekday,
			cte.is_working_hour,
			cte.excludedFromComfortAnalytics,
			CASE WHEN IFNULL(cte.excludedFromComfortAnalytics, false) = false THEN cte.comfort_score ELSE NULL END AS comfort_score,
            cte1.comfort_score AS comfort_score_1_month_ago,
            cte.last_captured_at_local,
            cte.last_refreshed_at_utc
		FROM cte_daily_agg cte
        LEFT JOIN cte_daily_agg cte1
               ON (cte1.building_id = cte.building_id)
              AND (cte1.date = DATEADD('month',-1,cte.date))
			  AND (cte1.is_working_hour = cte.is_working_hour)
			  AND IFNULL(cte.excludedFromComfortAnalytics, false) = IFNULL(cte1.excludedFromComfortAnalytics, false)
;