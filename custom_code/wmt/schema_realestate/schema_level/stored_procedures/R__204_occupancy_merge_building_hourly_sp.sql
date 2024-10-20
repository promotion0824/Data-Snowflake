-- ------------------------------------------------------------------------------------------------------------------------------
-- Merge into table from view
-- ------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE transformed.occupancy_merge_building_hourly_sp()
    RETURNS STRING
    LANGUAGE SQL
  AS
    $$
	  MERGE INTO transformed.occupancy_building_hourly AS tgt
		USING ( 
			WITH watermark AS 
				(
				  SELECT
					  IFNULL(MAX(datetime_local_hour),'2024-01-01') AS max_datetime_local_hour,
				  FROM transformed.occupancy_building_hourly
				)
    , cte_hourly AS (
    SELECT DISTINCT
        t.building_id, 
        t.model_id, 
        t.trend_id,
        ts.external_id,
        ts.date_local,
        DATE_TRUNC('HOUR',ts.timestamp_local) AS datetime_local_hour,
        LAST_VALUE(ts.telemetry_value ) OVER (PARTITION BY datetime_local_hour, ts.trend_id ORDER BY ts.timestamp_local) AS end_of_hour_value,
        MAX(ts.timestamp_local) OVER () AS last_captured_at_local
    FROM transformed.occupancy_building_twins t
    JOIN transformed.telemetry ts ON t.trend_id = ts.trend_id
    WHERE ts.timestamp_local >= (SELECT DATEADD(DAY,-3,IFNULL(MAX(max_datetime_local_hour),'2024-01-01')) FROM watermark) -- need to go back 3 days; there is a large latency in receing this data
    )
    SELECT
        ts.building_id,
        t.building_name,
        ts.model_id,
        t.capability_id,
        t.capability_name,
        ts.trend_id,
        ts.date_local,
        ts.datetime_local_hour,
        ts. external_id,
        dh.hour_num,
        dh.day_name,
        dh.day_of_week,
        ts.end_of_hour_value,
        LAG(ts.end_of_hour_value, 1, 0) OVER (PARTITION BY ts.building_id, ts.model_id ORDER BY ts.datetime_local_hour) AS previous_hour_value,
        GREATEST(0, end_of_hour_value - previous_hour_value)  AS hourly_incremental,
        dh.is_weekday,
        IFF(HOUR(dh.date_time_hour) >= COALESCE(working_hours.default_value:hourStart,8) 
            AND HOUR(dh.date_time_hour) <  COALESCE(working_hours.default_value:hourEnd, 18), true, false) AS is_working_hour,
        last_captured_at_local
    FROM cte_hourly ts
    JOIN transformed.occupancy_building_twins t ON ts.trend_id = t.trend_id
    JOIN transformed.date_hour dh ON ts.datetime_local_hour = dh.date_time_hour
    LEFT JOIN transformed.site_defaults working_hours
      ON (t.site_id = working_hours.site_id AND working_hours.type = 'WorkingHours')
    AND (working_hours._valid_from <= dh.date_time_hour AND working_hours._valid_to >= dh.date_time_hour)
	)
	 AS src
        ON (
                tgt.trend_id = src.trend_id
            AND tgt.datetime_local_hour = src.datetime_local_hour
            )
	  WHEN MATCHED 
	  THEN
		UPDATE 
		SET
				tgt.end_of_hour_value = src.end_of_hour_value,
        tgt.previous_hour_value = src.previous_hour_value,
        tgt.hourly_incremental = src.hourly_incremental,
				tgt.last_captured_at_local = src.last_captured_at_local
	  WHEN NOT MATCHED THEN
		INSERT (
            date_local,
            datetime_local_hour,
            day_name,
            day_of_week,
            hour_num,
            trend_id,
            external_id,
            end_of_hour_value,
            previous_hour_value,
            hourly_incremental,
            is_weekday,
            last_captured_at_local
		)
		VALUES (
            src.date_local,
            src.datetime_local_hour,
            src.day_name,
            src.day_of_week,
            src.hour_num,
            src.trend_id,
            src.external_id,
            src.end_of_hour_value,
            src.previous_hour_value,
            src.hourly_incremental,
            src.is_weekday,
            src.last_captured_at_local
		);
    $$
;