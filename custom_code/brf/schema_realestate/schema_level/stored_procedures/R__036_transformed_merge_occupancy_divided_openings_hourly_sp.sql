-- ******************************************************************************************************************************
-- Stored procedure that incrementally populates the aggregate table
-- This is called via transformed.merge_agg_electrical_metering_daily_tk
-- USAGE:  CALL transformed.merge_agg_electrical_metering_daily_sp(SYSTEM$CURRENT_USER_TASK_NAME());
-- ******************************************************************************************************************************
CREATE OR REPLACE PROCEDURE transformed.merge_occupancy_divided_openings_hourly_sp()
    RETURNS STRING
    LANGUAGE SQL
  AS
    $$
	  BEGIN
		  MERGE INTO transformed.occupancy_divided_openings_hourly AS tgt
		  USING (
				  WITH watermark AS 
					(
				  SELECT
					IFNULL(MAX(enqueued_at),'2019-01-01') AS max_date
				  FROM transformed.occupancy_divided_openings_hourly occ
					)
                SELECT
                    ts.date_local,
                    DATE_TRUNC('hour',ts.timestamp_local) AS date_time_local_hour,
                    ts.trend_id,
                    SUM(ts.telemetry_value) AS telemetry_value,
                    MAX(ts.enqueued_at) AS enqueued_at
				FROM transformed.telemetry  ts
				WHERE ts.enqueued_at > (SELECT COALESCE(max_date,'2018-01-01') FROM watermark)
                  AND EXISTS (SELECT 1 FROM transformed.occupancy_divided_openings_assets a WHERE a.trend_id = ts.trend_id)
                GROUP BY 
                ts.date_local,
                date_time_local_hour,
                ts.trend_id
          ) AS src
				  ON (    
					      tgt.date_time_local_hour = src.date_time_local_hour
					  AND tgt.trend_id = src.trend_id
					 )
		  WHEN MATCHED THEN
			UPDATE 
			SET
					tgt.telemetry_value = src.telemetry_value,
					tgt.enqueued_at = src.enqueued_at
					-- tgt._last_updated_by_task = :task_name,
					-- tgt.last_captured_at_local = src.last_captured_at_local,
					-- tgt.last_captured_at_utc = src.last_captured_at_utc,
					-- tgt.last_refreshed_at_utc = COALESCE(src.last_refreshed_at_utc,SYSDATE())
		  WHEN NOT MATCHED THEN
			INSERT (
					date_local,
                    date_time_local_hour,
					trend_id,
                    telemetry_value,
                    enqueued_at
					-- _created_at,
					-- _created_by_task,
                    -- _last_updated_by_task,
                    -- last_captured_at_local,
                    -- last_captured_at_utc,
                    -- last_refreshed_at_utc
			)
			VALUES (
					src.date_local,
                    src.date_time_local_hour,
					src.trend_id,
                    src.telemetry_value,
                    src.enqueued_at
					-- SYSDATE(), 
					-- :task_name,
                    -- :task_name,
					-- src.last_captured_at_local,
					-- src.last_captured_at_utc,
					-- SYSDATE()
			);
	END	
    $$
;