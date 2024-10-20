

CREATE OR REPLACE VIEW transformed.occupancy_doors_15_minute AS
SELECT
    ts.site_id,
    p.space_type,
	p.space_id,
    p.level_id,
    ts.date_local,
    ts.time_local_15min,
    ts.date_time_local_15min,
    ts.date_time_local_hour,
    IFF(HOUR(date_time_local_15min) >= COALESCE(working_hours.default_value:hourStart,8) 
    AND HOUR(date_time_local_15min) <  COALESCE(working_hours.default_value:hourEnd, 18), true, false) AS is_working_hour,
    d.is_weekday,
    SUM(CASE WHEN p.model_id_capability = 'dtmi:com:willowinc:EnteringPeopleCountSensor;1'
         THEN ts.telemetry_value 
         ELSE NULL
    END) AS entering_count,
    SUM(CASE WHEN p.model_id_capability = 'dtmi:com:willowinc:LeavingPeopleCountSensor;1'
         THEN ts.telemetry_value 
         ELSE NULL
    END) AS leaving_count,
    IFNULL(entering_count,0) - IFNULL(leaving_count,0) AS occupancy,
    MAX(ts.timestamp_local) AS last_captured_at_local,
    MAX(ts.timestamp_utc)  AS last_captured_at_utc
FROM transformed.time_series_enriched ts
JOIN transformed.occupancy_peoplecount p ON p.site_id = ts.site_id AND p.trend_id = ts.trend_id
JOIN transformed.dates d 
    ON (ts.date_local = d.date)
LEFT JOIN transformed.site_defaults working_hours
    ON (ts.site_id = working_hours.site_id AND working_hours.type = 'WorkingHours')
    AND (working_hours._valid_from <= ts.date_time_local_15min AND working_hours._valid_to >= ts.date_time_local_15min)
WHERE   ts.timestamp_utc >= '2022-09-20'
  AND p.model_id_capability IN ('dtmi:com:willowinc:EnteringPeopleCountSensor;1','dtmi:com:willowinc:LeavingPeopleCountSensor;1')
GROUP BY     
    ts.site_id,
    p.space_type,
	p.space_id,
    p.level_id,
    ts.date_local,
    ts.time_local_15min,
    ts.date_time_local_15min,
    ts.date_time_local_hour,
    d.is_weekday,
    is_working_hour
;

CREATE OR REPLACE VIEW published.occupancy_doors_15_minute AS
WITH cte_space AS (
    SELECT DISTINCT
        p.space_type,
		p.space_id,
        p.floor_id,
        p.level_name,
        p.level_id,
        p.site_id
    FROM transformed.occupancy_peoplecount p
)
SELECT DISTINCT
    ts.date_local,
    ts.date_time_local_hour,
    ts.time_local_15min,
    ts.date_time_local_15min,
    IFF(HOUR(date_time_local_15min) >= COALESCE(working_hours.default_value:hourStart,8) 
    AND HOUR(date_time_local_15min) <  COALESCE(working_hours.default_value:hourEnd, 18), true, false) AS is_working_hour,
    d.is_weekday,
    ts.entering_count,
    ts.leaving_count,
    ts.occupancy AS occupancy_15_minute,
    occupancy_15_minute + IfNULL(LAG(ts.occupancy,1) OVER (PARTITION BY ts.date_local,s.space_id ORDER BY ts.date_time_local_15min),0) AS  occupancy_30_minute,
    s.space_type,
	s.space_id,
    s.floor_id,
    s.level_name,
    s.level_id,
    s.site_id,
    MAX(ts.last_captured_at_local) OVER (PARTITION BY ts.site_id) AS last_captured_at_local,
    MAX(ts.last_captured_at_utc) OVER (PARTITION BY ts.site_id) AS last_captured_at_utc,
	MAX(ts.last_captured_at_utc) OVER () AS last_refreshed_at_utc
FROM transformed.occupancy_doors_15_minute ts
JOIN cte_space s 
  ON (s.space_id = ts.space_id)
 AND (s.level_id = ts.level_id)
JOIN transformed.dates d 
    ON (ts.date_local = d.date)
LEFT JOIN transformed.site_defaults working_hours
    ON (ts.site_id = working_hours.site_id AND working_hours.type = 'WorkingHours')
    AND (working_hours._valid_from <= ts.date_time_local_15min AND working_hours._valid_to >= ts.date_time_local_15min)
;