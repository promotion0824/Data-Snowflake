-- ******************************************************************************************************************************
-- Task to aggregate to 15 minute granularity
-- ******************************************************************************************************************************

ALTER TASK IF EXISTS transformed.merge_twins_stream_tk SUSPEND;
CREATE OR REPLACE TASK transformed.merge_agg_hvac_occupancy_15minute_tk
   USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
   USER_TASK_TIMEOUT_MS = 1200000
   AFTER transformed.create_table_transformed_capabilities_hvac_occupancy_tk
AS
	  MERGE INTO transformed.agg_hvac_occupancy_15minute AS tgt
	  USING (    
	  WITH watermark AS 
		(
		  SELECT IFNULL(MAX(date_local),YEAR(current_date)::char(4)||'-01'||'-01') AS max_date
		  FROM transformed.agg_hvac_occupancy_15minute
		)
		  SELECT 
			  ts.site_id,
			  ts.trend_id,
			  ts.date_local,
			  ts.time_local_15min,
			  ts.date_time_local_15min,
			  AVG(ts.telemetry_value) AS avg_value_15minute,
			  MIN(ts.telemetry_value) AS min_value_15minute,
			  MAX(ts.telemetry_value) AS max_value_15minute,
			  MAX(ts.last_value_15min) AS last_value_15minute
		  FROM transformed.time_series_enriched ts
		  WHERE EXISTS (SELECT * FROM transformed.capabilities_hvac_occupancy o WHERE site_id = ts.site_id AND trend_id = ts.trend_id)
			AND ts.date_local >= (SELECT max_date FROM watermark)
		  GROUP BY
			  ts.trend_id,
			  ts.site_id,
			  ts.date_local,
			  ts.time_local_15min,
			  ts.date_time_local_15min
		  ) AS src
		  ON (    tgt.site_id = src.site_id
			  AND tgt.trend_id = src.trend_id
			  AND tgt.date_time_local_15min = src.date_time_local_15min
			 )
  WHEN MATCHED THEN
    UPDATE 
    SET
			tgt.date_local = src.date_local,
			tgt.time_local_15min = src.time_local_15min,
			tgt.avg_value_15minute = src.avg_value_15minute,
			tgt.min_value_15minute = src.min_value_15minute,
			tgt.max_value_15minute = src.max_value_15minute,
			tgt.last_value_15minute = src.last_value_15minute,
			tgt._last_updated_at = SYSDATE(),
			tgt._last_updated_by_task = SYSTEM$CURRENT_USER_TASK_NAME()
  WHEN NOT MATCHED THEN
    INSERT (
			site_id,
			trend_id,
			date_local,
			time_local_15min,
			date_time_local_15min,
			avg_value_15minute,
			min_value_15minute,
			max_value_15minute,
			last_value_15minute,
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
			src.avg_value_15minute,
			src.min_value_15minute,
			src.max_value_15minute,
			src.last_value_15minute,
            SYSDATE(), 
            SYSTEM$CURRENT_USER_TASK_NAME(),
            SYSDATE(),
            SYSTEM$CURRENT_USER_TASK_NAME()
    )
;
-- ALTER TASK transformed.merge_agg_hvac_occupancy_15minute_tk RESUME
ALTER TASK IF EXISTS transformed.merge_twins_stream_tk RESUME;

