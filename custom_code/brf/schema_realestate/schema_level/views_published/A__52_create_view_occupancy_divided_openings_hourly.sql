-- ******************************************************************************************************************************
-- Create view
-- ******************************************************************************************************************************
CREATE OR REPLACE VIEW published.occupancy_divided_openings_hourly AS
SELECT
 ts.date_local,
 ts.date_time_local_hour,
 IFF(HOUR(ts.date_time_local_hour) >= COALESCE(working_hours.default_value:hourStart,8) 
 AND HOUR(ts.date_time_local_hour) <  COALESCE(working_hours.default_value:hourEnd, 18), true, false) AS is_working_hour,
 d.is_weekday,
 ts.trend_id,
 ts.telemetry_value,
 SUM(ts.telemetry_value) OVER (PARTITION BY ts.date_local, ts.trend_id ORDER BY date_time_local_hour ) AS daily_count,
 a.model_id,
 a.capability_name,
 a.capability_id,
 a.model_id_asset,
 a.asset_id,
 a.entrance_name,
 a.relationship_name,
 a.occupancy_zone,
 a.space_type,
 a.building_id,
 a.building_name,
 a.site_id,
 a.site_name,
 ts.enqueued_at
FROM transformed.occupancy_divided_openings_hourly ts
JOIN transformed.occupancy_divided_openings_assets a 
  ON (ts.trend_id = a.trend_id)
JOIN transformed.dates d ON ts.date_local = d.date
LEFT JOIN transformed.site_defaults working_hours
     ON (a.site_id = working_hours.site_id AND working_hours.type = 'WorkingHours')
    AND (working_hours._valid_from <= ts.date_time_local_hour AND working_hours._valid_to >= ts.date_time_local_hour)
;