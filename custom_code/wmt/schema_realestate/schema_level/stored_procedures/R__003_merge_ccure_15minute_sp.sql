-- ------------------------------------------------------------------------------------------------------------------------------
-- Merge into table from all possible trend_id/15min combos left join with actuals from time series
-- The src query only includes date_time_local_15min greater than what is already there
-- ------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE transformed.merge_ccure_15minute_sp(task_name VARCHAR)
    RETURNS STRING
    LANGUAGE SQL
  AS
    $$
	  MERGE INTO transformed.ccure_15minute AS tgt
		USING ( 
			  WITH watermark AS 
				(
				  SELECT DISTINCT
					IFNULL(MAX(date_time_local_15min),'2019-01-01') AS max_15min
				  FROM transformed.ccure_15minute
				)
			SELECT DISTINCT
				 poss.date_local,
				 poss.time_local_15min,
				 poss.date_time_local_15min,
				 CASE WHEN poss.time_local_15min BETWEEN '08:00' AND '18:00' THEN 'During Business Hours' ELSE 'Outside Business Hours' END AS is_business_hours,
				 poss.site_id,
				 poss.trend_id,
				 act.last_value_15min AS last_15min_value,
 				 LAG(act.last_value_15min, 1, 0) OVER (PARTITION BY poss.trend_id,poss.date_local ORDER BY poss.trend_id, poss.date_local,poss.time_local_15min) AS prev_15min_value,
				 GREATEST(last_15min_value - prev_15min_value,0) AS diff_to_prev
				 FROM transformed.ccure_trend_id_15minute poss
					LEFT JOIN transformed.ccure_time_series_15minute act
						   ON (poss.trend_id = act.trend_id)
						  AND (poss.date_time_local_15min = act.date_time_local_15min)
				WHERE 
					  poss.date_time_local_15min > (SELECT DATEADD('d',-1,max_15min) FROM watermark)
	)
	 AS src
			  ON (
				      tgt.trend_id = src.trend_id
				  AND tgt.date_time_local_15min = src.date_time_local_15min
				 )
	  WHEN MATCHED 
	  	   AND 	IFNULL(tgt.last_15min_value,0) != IFNULL(src.last_15min_value,0)
	  	   AND  IFNULL(tgt.prev_15min_value,0) != IFNULL(src.prev_15min_value,0)
	  	   AND  IFNULL(tgt.diff_to_prev,0) != IFNULL(src.diff_to_prev,0)
	  THEN
		UPDATE 
		SET
				tgt.last_15min_value = src.last_15min_value,
				tgt.prev_15min_value = src.prev_15min_value,
				tgt.diff_to_prev = src.diff_to_prev,
				tgt._last_updated_at = SYSDATE(),
				tgt._last_updated_by_task = :task_name
	  WHEN NOT MATCHED THEN
		INSERT (
				site_id,
				trend_id,
				date_local,
				time_local_15min,
				date_time_local_15min,
				is_business_hours,
				last_15min_value,
				prev_15min_value,
				diff_to_prev,
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
				src.is_business_hours,
				src.last_15min_value,
				src.prev_15min_value,
				src.diff_to_prev,
				SYSDATE(), 
				:task_name,
				SYSDATE(),
				:task_name
		);
    $$
;