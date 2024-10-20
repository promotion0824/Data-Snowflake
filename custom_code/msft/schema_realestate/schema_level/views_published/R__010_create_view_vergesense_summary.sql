-- ------------------------------------------------------------------------------------------------------------------------------
-- create View for reporting
-- ------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE VIEW published.vergesense_summary AS
SELECT
    ts.date_local,
    ts.start_of_hour,
    IFF(HOUR(start_of_hour) >= COALESCE(working_hours.default_value:hourStart,8) 
    AND HOUR(start_of_hour) <  COALESCE(working_hours.default_value:hourEnd, 18), true, false) AS is_working_hour,
    d.is_weekday,
	ts.trend_id,
    ts.capability_name,
    ts.average,
    ts.minimum,
    ts.maximum,
    ts.type,
    ts.off_count,
	ts.on_count,
    ts.model_id,
    ts.asset_id,
    v.asset_name,
    v.model_id_asset,
    v.space_id,
    v.space_name,
    v.space_type,
	v.seating_capacity,
    v.usable_area,
    v.floor_id,
	v.level_id,
	v.level_name,
	v.floor_code,
	v.floor_sort_order,
    v.building_id,
    v.building_name,
	v.site_id,
	v.site_name,
	MAX(ts.start_of_hour) OVER (PARTITION BY ts.trend_id) AS last_captured_at_local,
	MAX(ts._last_updated_at) OVER () AS last_refreshed_at_utc,
    CONVERT_TIMEZONE( 'UTC',v.time_zone, last_refreshed_at_utc) AS last_refreshed_at_local
FROM transformed.vergesense_assets v
    LEFT JOIN transformed.vergesense_hourly_summary ts
           ON (ts.trend_id = v.trend_id )
          AND (ts.asset_id = v.asset_id )
         JOIN transformed.dates d 
           ON (ts.date_local = d.date)
    LEFT JOIN transformed.site_defaults working_hours
        ON (ts.site_id = working_hours.site_id AND working_hours.type = 'WorkingHours')
        AND (working_hours._valid_from <= ts.start_of_hour AND working_hours._valid_to >= ts.start_of_hour)
;