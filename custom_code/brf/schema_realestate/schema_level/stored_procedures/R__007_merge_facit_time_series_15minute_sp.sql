-- ------------------------------------------------------------------------------------------------------------------------------
-- Merge into table from time series aggregate to 15 minute level
-- The src only includes date_time_local_15min greater than what is already there
-- ------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE transformed.merge_facit_time_series_15minute_sp(task_name VARCHAR)
    RETURNS STRING
    LANGUAGE SQL
  AS
    $$
	BEGIN
	  MERGE INTO transformed.facit_time_series_15minute AS tgt
		USING ( 
			  WITH watermark AS 
				(
				  SELECT DISTINCT
					IFNULL(MAX(date_time_local_15min),'2023-01-01') AS max_15min
				  FROM transformed.facit_time_series_15minute
				)
			SELECT
				 ts.site_id,
				 ts.trend_id,
				 ts.date_local,
				 ts.time_local_15min,
				 ts.date_time_local_15min,
				 MEDIAN(ts.telemetry_value) AS median_value_15min,
				 MAX(ts.telemetry_value) AS max_value_15min
			  FROM transformed.time_series_enriched ts
			  WHERE EXISTS (Select site_id, trend_id FROM transformed.facit_trend_ids cc WHERE ts.site_id = cc.site_id AND ts.trend_id = cc.trend_id)
			    AND timestamp_utc > (SELECT DATEADD('d',-1,max_15min) FROM watermark)
				AND ts.date_time_local_15min >= (SELECT max_15min FROM watermark)
			  GROUP BY
				 ts.site_id,
				 ts.trend_id,
				 ts.date_local,
				 ts.time_local_15min,
				 ts.date_time_local_15min
	)
	 AS src
			  ON (    tgt.site_id = src.site_id
				  AND tgt.trend_id = src.trend_id
				  AND tgt.date_time_local_15min = src.date_time_local_15min
				 )
	  WHEN MATCHED THEN
		UPDATE 
		SET
				tgt.median_value_15min = src.median_value_15min,
				tgt.max_value_15min = src.max_value_15min,
				tgt.date_local = src.date_local,
				tgt.time_local_15min = src.time_local_15min,
				tgt._last_updated_at = SYSDATE(),
				tgt._last_updated_by_task = :task_name
	  WHEN NOT MATCHED THEN
		INSERT (
				site_id,
				trend_id,
				date_local,
				time_local_15min,
				date_time_local_15min,
				median_value_15min,
				max_value_15min,
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
				src.median_value_15min,
				src.max_value_15min,
				SYSDATE(), 
				:task_name,
				SYSDATE(),
				:task_name
		);
					
	END
    $$
;