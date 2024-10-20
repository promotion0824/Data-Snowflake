-- ------------------------------------------------------------------------------------------------------------------------------
-- Merge into table from time series aggregate to 15 minute level
-- The src only includes date_time_local_15min greater than what is already there
-- ------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE transformed.merge_access_control_time_series_15minute_sp(task_name VARCHAR)
    RETURNS STRING
    LANGUAGE SQL
  AS
    $$
	BEGIN
	  MERGE INTO transformed.access_control_time_series_15minute AS tgt
		USING ( 
			  WITH watermark AS 
				(
				  SELECT DISTINCT
					IFNULL(MAX(max_enqueued_at_utc),'2019-01-01') AS max_15min
				  FROM transformed.access_control_time_series_15minute
				)
			SELECT DISTINCT
				 ts.site_id,
				 ts.trend_id,
				 ts.date_local,
				 ts.time_local_15min,
				 ts.date_time_local_15min,
				 MAX( ts.enqueued_at ) OVER ( PARTITION BY ts.trend_id, date_time_local_15min) AS max_enqueued_at_utc,
				 LAST_VALUE( ts.telemetry_value ) OVER ( PARTITION BY ts.trend_id, date_time_local_15min ORDER BY ts.trend_id, ts.timestamp_local) AS last_value_15min
			  FROM transformed.time_series_enriched ts
			  WHERE EXISTS (Select site_id, trend_id FROM transformed.access_control_trend_ids cc WHERE ts.site_id = cc.site_id AND ts.trend_id = cc.trend_id)
			    AND timestamp_utc > (SELECT DATEADD('d',-1,max_15min) FROM watermark)
				AND ts.date_time_local_15min >= (SELECT max_15min FROM watermark)
	)
	 AS src
			  ON (
				      tgt.trend_id = src.trend_id
				  AND tgt.date_time_local_15min = src.date_time_local_15min
				 )
	  WHEN MATCHED THEN
		UPDATE 
		SET
				tgt.last_value_15min = src.last_value_15min,
				tgt.max_enqueued_at_utc = src.max_enqueued_at_utc,
				tgt._last_updated_at = SYSDATE(),
				tgt._last_updated_by_task = :task_name
	  WHEN NOT MATCHED THEN
		INSERT (
				site_id,
				trend_id,
				date_local,
				time_local_15min,
				date_time_local_15min,
				last_value_15min,
				max_enqueued_at_utc,
				_created_at,
				_created_by_task,
				_last_updated_at,
				_last_updated_by_task
		)
		VALUES (
				src.site_id,
				src.trend_id,
				src.date_local,
				src.time_local_15min,
				src.date_time_local_15min,
				src.last_value_15min,
				src.max_enqueued_at_utc,
				SYSDATE(), 
				:task_name,
				SYSDATE(),
				:task_name
		);
					
	END
    $$
;