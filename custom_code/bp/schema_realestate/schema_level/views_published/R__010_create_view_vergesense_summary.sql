-- ------------------------------------------------------------------------------------------------------------------------------
-- create View for reporting
-- ------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW published.vergesense_summary AS

WITH cte_last_refreshed AS (
    SELECT max(_last_updated_at) AS _last_updated_at FROM transformed.vergesense_hourly_summary
)

SELECT
    ts.date_local,
    ts.date_time_local_hour,
    IFF(HOUR(date_time_local_hour) >= COALESCE(working_hours.default_value:hourStart,8) 
    AND HOUR(date_time_local_hour) <  COALESCE(working_hours.default_value:hourEnd, 18), true, false) AS is_working_hour,
	ts.trend_id,
    CASE WHEN DAYOFWEEKISO(date_local) > 5 THEN false ELSE true END AS is_weekday,
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
	ts.date_time_local_hour AS last_captured_at_local,
	lr._last_updated_at AS last_refreshed_at_utc,
    CONVERT_TIMEZONE( 'UTC',v.time_zone, last_refreshed_at_utc) AS last_refreshed_at_local
FROM transformed.vergesense_hourly_summary ts
    JOIN transformed.vergesense_assets v
           ON (ts.trend_id = v.trend_id )
          AND (ts.asset_id = v.asset_id )
    LEFT JOIN transformed.site_defaults working_hours
        ON (ts.site_id = working_hours.site_id AND working_hours.type = 'WorkingHours')
        AND (working_hours._valid_from <= ts.date_time_local_hour AND working_hours._valid_to >= ts.date_time_local_hour)
    CROSS JOIN cte_last_refreshed lr
;