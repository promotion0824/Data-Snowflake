-- ------------------------------------------------------------------------------------------------------------------------------
-- Merge into table from all possible trend_id/15min combos 
-- The src only includes date_time_local_15min greater than what is already there
-- ------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE transformed.merge_access_control_trend_id_15minute_sp(task_name VARCHAR)
    RETURNS STRING
    LANGUAGE SQL
  AS
    $$
	BEGIN
	
	  MERGE INTO transformed.access_control_trend_id_15minute AS tgt
		USING ( 
			  WITH watermark AS 
				(
				  SELECT DISTINCT
					IFNULL(MAX(date_time_local_15min),'2019-01-01') AS max_15min
				  FROM transformed.access_control_trend_id_15minute
				)
			SELECT DISTINCT
			    d.date AS date_local,
			    i.time_local_15min,
                TO_TIMESTAMP(CONCAT(TO_CHAR(date_local,'YYYY-MM-DD'),  ' ',  time_local_15min)) as date_time_local_15min,
                ts.site_id,
                ts.trend_id
			FROM utils.dates d 
			JOIN (SELECT site_id,trend_id,min(date_local) AS min_date, max(date_local) AS max_date
				  FROM transformed.access_control_time_series_15minute GROUP BY site_id,trend_id
				 ) ts ON d.date Between ts.min_date and ts.max_date
			CROSS JOIN utils.intervals_15minute i
			WHERE 
                    date_time_local_15min > (SELECT DATEADD('d',-1,max_15min) FROM watermark)
	)
	 AS src
			  ON (    tgt.site_id = src.site_id
				  AND tgt.trend_id = src.trend_id
				  AND tgt.date_time_local_15min = src.date_time_local_15min
				 )
	  WHEN MATCHED THEN
		UPDATE 
		SET
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
				SYSDATE(), 
				:task_name,
				SYSDATE(),
				:task_name
		);
		
		CREATE OR REPLACE TABLE transformed.access_control_trend_ids AS SELECT * FROM transformed.access_control_trend_ids_v;
		
	END;
    $$
;
