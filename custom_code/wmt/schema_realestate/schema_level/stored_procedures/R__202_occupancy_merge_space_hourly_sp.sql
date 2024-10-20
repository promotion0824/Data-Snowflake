-- ------------------------------------------------------------------------------------------------------------------------------
-- Merge into table from all possible trend_id/15min combos left join with actuals from time series
-- The src query only includes date_time_local_15min greater than what is already there
-- ------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE transformed.occupancy_merge_space_hourly_sp()
    RETURNS STRING
    LANGUAGE SQL
  AS
    $$

	  MERGE INTO transformed.occupancy_space_hourly AS tgt
		USING ( 
			WITH watermark AS 
				(
				  SELECT DISTINCT
					IFNULL(MAX(last_refreshed_at_utc),'2019-01-01') AS max_update_utc,
				  FROM transformed.occupancy_space_hourly
				)
            ,cte_telemetry AS (
                SELECT
                    date_local,
                    external_id,
                    trend_id,
                    telemetry_value,
                    timestamp_utc,
                    valid_from, 
                    valid_to,
                    last_refreshed_at_utc
            from transformed.occupancy_space_telemetry ts
            WHERE trend_id > ''
              AND ts.last_refreshed_at_utc > (SELECT max_update_utc FROM watermark)
            UNION ALL
                SELECT
                    date_local,
                    external_id,
                    trend_id,
                    telemetry_value,
                    timestamp_utc,
                    valid_from, 
                    valid_to,
                    last_refreshed_at_utc
            from transformed.occupancy_space_telemetry ts
            WHERE trend_id IS NULL
              AND ts.last_refreshed_at_utc > (SELECT max_update_utc FROM watermark)
            )
            ,cte_datehour AS (
                SELECT date, date_time_hour, hour_num, day_name, day_of_week, is_weekday
                FROM transformed.date_hour
                WHERE date >= (SELECT MIN(date_local) FROM transformed.occupancy_space_telemetry)
                AND date <= DATEADD('DAY', 1, SYSDATE())
            )
            SELECT DISTINCT
                dh.date AS date_local,
                dh.date_time_hour AS datetime_local_hour,
                dh.day_name,
                dh.day_of_week,
                hour_num,
                ts.external_id,
                ts.trend_id,
                MAX(ts.telemetry_value) OVER (PARTITION BY datetime_local_hour,ts.external_id) AS occupancy_count,
                dh.is_weekday,
                MAX(ts.timestamp_utc) OVER () AS last_captured_at_ut,
                MAX(ts.last_refreshed_at_utc) OVER () AS last_refreshed_at_utc
            FROM cte_telemetry ts
            JOIN cte_datehour dh 
              ON dh.date_time_hour BETWEEN ts.valid_from AND IFNULL(ts.valid_to, SYSDATE())
              OR DATE_TRUNC('HOUR',valid_to) = DATE_TRUNC('HOUR',dh.date_time_hour) -- within same hour
	)
	 AS src
        ON (
              tgt.datetime_local_hour = src.datetime_local_hour
          AND IFNULL(tgt.external_id,'')  = IFNULL(src.external_id,'') 
          AND IFNULL(tgt.trend_id,'') = IFNULL(src.trend_id,'')
            )
	  WHEN MATCHED 
	  THEN
		UPDATE 
		SET
				tgt.occupancy_count = src.occupancy_count,
				tgt.last_captured_at_ut = src.last_captured_at_ut,
				tgt.last_refreshed_at_utc = src.last_refreshed_at_utc
	  WHEN NOT MATCHED THEN
		INSERT (
            date_local,
            datetime_local_hour,
            day_name,
            day_of_week,
            hour_num,
            external_id,
            trend_id,
            occupancy_count,
            is_weekday,
            last_captured_at_ut,
            last_refreshed_at_utc
		)
		VALUES (
            src.date_local,
            src.datetime_local_hour,
            src.day_name,
            src.day_of_week,
            src.hour_num,
            src.external_id,
            src.trend_id,
            src.occupancy_count,
            src.is_weekday,
            src.last_captured_at_ut,
            src.last_refreshed_at_utc
		);
    $$
;