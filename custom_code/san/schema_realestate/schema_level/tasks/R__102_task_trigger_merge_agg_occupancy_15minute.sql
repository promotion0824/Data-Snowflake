-- ------------------------------------------------------------------------------------------------------------------------------
-- Task that aggregates to the 15 minute level 
-- ------------------------------------------------------------------------------------------------------------------------------
SET time_zone = (SELECT TOP 1 time_zone FROM transformed.sites GROUP BY time_zone ORDER BY count(*) DESC);
SET time_zone_default = (SELECT CASE current_region()
    WHEN 'AZURE_AUSTRALIAEAST' THEN 'Australia/Sydney'
    WHEN 'AZURE_EASTUS2' THEN 'America/New_York'
    WHEN 'AZURE_WESTEUROPE' THEN 'Europe/Paris'
    ELSE NULL
    END);
SET task_schedule = 'USING CRON 0 */3 * * * ' || COALESCE($time_zone,$time_zone_default,'Etc/UTC');

CREATE OR REPLACE TASK transformed.merge_agg_occupancy_15minute_tk
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
  USER_TASK_TIMEOUT_MS = 1200000
  SUSPEND_TASK_AFTER_NUM_FAILURES = 5
  ERROR_INTEGRATION = error_{{ environment }}_nin
  SCHEDULE = $task_schedule
AS
	  MERGE INTO transformed.agg_occupancy_15minute AS tgt
	  USING (  
	  WITH watermark AS 
		(
		  SELECT IFNULL(MAX(date_local),YEAR(current_date)::char(4)||'-01'||'-01') AS max_date
		  FROM transformed.agg_occupancy_15minute
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
		  WHERE EXISTS (SELECT * FROM transformed.occupancy o WHERE trend_id = ts.trend_id)
		    AND ts.timestamp_utc > (SELECT max_date-1 FROM watermark)
			AND ts.date_local >= (SELECT max_date FROM watermark)
		  GROUP BY
			  ts.trend_id,
			  ts.site_id,
			  ts.date_local,
			  ts.time_local_15min,
			  ts.date_time_local_15min
		  ) AS src
		  ON (    tgt.trend_id = src.trend_id
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
    );
ALTER TASK transformed.merge_agg_occupancy_15minute_tk RESUME;