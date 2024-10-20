-- ------------------------------------------------------------------------------------------------------------------------------
-- Merge into table from time series aggregate to hour level
-- The src only includes date_time_local_hour greater than what is already there
-- ------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE transformed.merge_time_series_occupancy_sp(task_name VARCHAR)
    RETURNS STRING
    LANGUAGE SQL
  AS
    $$
	BEGIN
	  MERGE INTO transformed.time_series_occupancy AS tgt
		USING ( 
			  WITH watermark AS 
				(
				  SELECT DISTINCT
					IFNULL(MAX(date_time_local_hour),'2019-01-01') AS max_hour
				  FROM transformed.time_series_occupancy
				)
				SELECT DISTINCT
					 ts.site_id,
					 ts.trend_id,
					 ts.date_local,
					 ts.date_time_local_hour,
					 SUM(ts.telemetry_value) as sum_telemetry_value
				FROM transformed.time_series_enriched ts
				WHERE EXISTS (Select site_id, trend_id FROM transformed.occupancy_occupancysensor cc WHERE ts.site_id = cc.site_id AND ts.trend_id = cc.trend_id)
					AND timestamp_utc > (SELECT DATEADD('d',-1,max_hour) FROM watermark)
					AND ts.date_time_local_hour >= (SELECT max_hour FROM watermark)
				GROUP BY 
					ts.site_id,
					ts.trend_id,
					ts.date_local,
					ts.date_time_local_hour
				)
	 AS src
			  ON (    tgt.site_id = src.site_id
				  AND tgt.trend_id = src.trend_id
				  AND tgt.date_time_local_hour = src.date_time_local_hour
				 )
	  WHEN MATCHED THEN
		UPDATE 
		SET
				tgt.sum_telemetry_value = src.sum_telemetry_value,
				tgt.date_local = src.date_local,
				tgt._last_updated_at = SYSDATE(),
				tgt._last_updated_by_task = :task_name
	  WHEN NOT MATCHED THEN
		INSERT (
				site_id,
				trend_id,
				date_local,
				date_time_local_hour,
				sum_telemetry_value,
				_created_at,
				_created_by_task,
				_last_updated_at,
				_last_updated_by_task
		)
		VALUES (
				src.site_id,
				src.trend_id,
				src.date_local,
				src.date_time_local_hour,
				src.sum_telemetry_value,
				SYSDATE(), 
				:task_name,
				SYSDATE(),
				:task_name
		);
					
	END
    $$
;