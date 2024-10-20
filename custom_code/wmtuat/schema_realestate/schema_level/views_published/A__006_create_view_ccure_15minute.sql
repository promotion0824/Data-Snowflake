-- ------------------------------------------------------------------------------------------------------------------------------
-- Create View
-- ------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE VIEW published.ccure_15minute AS 
SELECT 
	ts.date_local,
	ts.time_local_15min,
	ts.date_time_local_15min,
	ts.is_business_hours,  --redundant now; remove after dashboards are switched over
	ts.is_business_hours AS is_working_hour,
	cc.site_id,
	cc.site_name,
	ts.trend_id,
	ts.last_15min_value,
	ts.prev_15min_value,
	ts.diff_to_prev,
	cc.capability_id,
	cc.model_id,
	cc.capability_name,
	cc.capability_description,
	ts_1_weeek_ago.last_15min_value AS week_ago_15min_value,
	ts_1_weeek_ago.prev_15min_value AS week_ago_prev_15min_value,
	ts_1_weeek_ago.diff_to_prev AS week_ago_diff_to_prev,
	cc.asset_name,
	cc.category_name,
	cc.level_name,
    cc.floor_sort_order,
	cc.max_occupancy,
	MAX(ts.date_time_local_15min) OVER (PARTITION BY ts.trend_id) AS last_captured_at_local,
	MAX(ts._last_updated_at) OVER () AS last_refreshed_at_utc,
	CONVERT_TIMEZONE( 'UTC', cc.time_zone, MAX(ts._last_updated_at) OVER ()) AS last_refreshed_at_local
FROM transformed.ccure_15minute ts
	JOIN transformed.ccure_trend_ids cc 
	  ON (ts.trend_id = cc.trend_id)
    JOIN transformed.dates d 
      ON (ts.date_local = d.date)
LEFT JOIN transformed.ccure_15minute ts_1_weeek_ago
	  ON (ts.trend_id = ts_1_weeek_ago.trend_id)
     AND (DATEADD('week',-1, ts.date_time_local_15min) = ts_1_weeek_ago.date_time_local_15min)
WHERE ts.date_local >= '2024-01-01'
 AND ts.date_time_local_15min <= (SELECT MAX(date_time_local_15min) FROM transformed.ccure_time_series_15minute)
;