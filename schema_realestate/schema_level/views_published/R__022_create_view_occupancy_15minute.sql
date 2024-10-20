-- ******************************************************************************************************************************
-- Create view published.occupancy_15minute 
-- ******************************************************************************************************************************
CREATE OR REPLACE VIEW published.occupancy_15minute AS
  SELECT 
    ts.trend_id,
    o.building_id,
    o.building_name,
    o.site_id,
    ts.date_local,
    ts.time_local_15min,
    ts.date_time_local_15min,
    IFF(HOUR(date_time_local_15min) >= COALESCE(working_hours.default_value:hourStart,8) 
    AND HOUR(date_time_local_15min) <  COALESCE(working_hours.default_value:hourEnd, 18), true, false) AS is_working_hour,
    d.is_weekday,
    ts.avg_value_15minute,
    ts.min_value_15minute,
    ts.max_value_15minute,
    ts.last_value_15minute,
    ts.last_value_15minute - LAG(ts.last_value_15minute, 1, 0) OVER (
      PARTITION BY ts.trend_id, date_local
      ORDER BY 
        ts.trend_id,
        ts.time_local_15min,
        ts.last_value_15minute
    ) AS diff_to_prev,
    o.capability_name,
    o.model_id_capability AS model_id,
    o.capability_type,
    o.space_name,
    o.space_id,
    o.space_type,
    o.capacity,
    o.usable_area,
    o.level_name,
    CONVERT_TIMEZONE( 'UTC', o.time_zone, MAX(ts._last_updated_at) OVER ()) AS _last_updated_at
  FROM transformed.agg_occupancy_15minute ts
    JOIN transformed.occupancy o 
      ON (ts.trend_id = o.trend_id)
    JOIN transformed.dates d 
      ON (ts.date_local = d.date)
    LEFT JOIN transformed.site_defaults working_hours
        ON (ts.site_id = working_hours.site_id AND working_hours.type = 'WorkingHours')
        AND (working_hours._valid_from <= ts.date_time_local_15min AND working_hours._valid_to >= ts.date_time_local_15min)
  ;