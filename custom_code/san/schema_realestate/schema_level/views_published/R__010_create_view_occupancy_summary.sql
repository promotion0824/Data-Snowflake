-- ------------------------------------------------------------------------------------------------------------------------------
-- create View for reporting
-- ------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW published.occupancy_summary AS
SELECT
    ts.date_local,
    ts.date_time_local_hour,
    IFF(HOUR(date_time_local_hour) >= COALESCE(working_hours.default_value:hourStart,8) 
    AND HOUR(date_time_local_hour) <  COALESCE(working_hours.default_value:hourEnd, 18), true, false) AS is_working_hour,
    d.is_weekday,
	ts.trend_id,
    ts.capability_name,
    ts.sum,
    ts.average,
    ts.minimum,
    ts.maximum,
    ts.last_value_hour,
	ts.count,
    ts.type,
    ts.model_id,
    ts.asset_id,
    v.asset_name AS occupancy_zone,
    v.seating_capacity,
    v.space_name,
    v.tenant_name,
    v.tenant_id,
    v.tenant_unit_id,
    v.tenant_unit_name,
    v.floor_id,
	v.level_id,
	v.level_name,
	v.floor_sort_order,
    v.building_id,
    v.building_name,
	v.site_id,
	v.site_name,
	MAX(ts.date_time_local_hour) OVER (PARTITION BY ts.trend_id) AS last_captured_at_local,
	MAX(ts._last_updated_at) OVER () AS last_refreshed_at_utc,
    CONVERT_TIMEZONE( 'UTC',v.time_zone, last_refreshed_at_utc) AS last_refreshed_at_local
FROM transformed.occupancy_assets v
    LEFT JOIN transformed.occupancy_hourly_summary ts
           ON (ts.trend_id = v.trend_id )
          AND (ts.asset_id = v.asset_id )
         JOIN transformed.dates d 
           ON (ts.date_local = d.date)
    LEFT JOIN transformed.site_defaults working_hours
        ON (ts.site_id = working_hours.site_id AND working_hours.type = 'WorkingHours')
        AND (working_hours._valid_from <= ts.date_time_local_hour AND working_hours._valid_to >= ts.date_time_local_hour)
;