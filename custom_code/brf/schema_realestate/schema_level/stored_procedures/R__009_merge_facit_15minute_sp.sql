-- ------------------------------------------------------------------------------------------------------------------------------
-- Merge into table from all possible trend_id/15min combos left join with actuals from time series
-- The src query only includes date_time_local_15min greater than what is already there
-- ------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE transformed.merge_facit_15minute_sp(task_name VARCHAR)
    RETURNS STRING
    LANGUAGE SQL
  AS
    $$
	  MERGE INTO transformed.facit_15minute AS tgt
		USING ( 
			  WITH watermark AS 
				(
				  SELECT DISTINCT
					IFNULL(MAX(date_time_local_15min),'2019-01-01') AS max_15min
				  FROM transformed.facit_15minute
				)
			SELECT
				 poss.date_local,
				 poss.time_local_15min,
				 poss.date_time_local_15min,
				 CASE WHEN poss.time_local_15min BETWEEN '08:00' AND '18:00' THEN 'During Business Hours' ELSE 'Outside Business Hours' END AS is_business_hours,
				 poss.site_id,
				 poss.trend_id,
				 act.median_value_15min,
				 act.max_value_15min
				 FROM transformed.facit_trend_id_15minute poss
					LEFT JOIN transformed.facit_time_series_15minute act
						   ON (poss.site_id = act.site_id)
						  AND (poss.trend_id = act.trend_id)
						  AND (poss.date_time_local_15min = act.date_time_local_15min)
				WHERE 
					  poss.date_time_local_15min > (SELECT DATEADD('d',-1,max_15min) FROM watermark)
	)
	 AS src
			  ON (    tgt.site_id = src.site_id
				  AND tgt.trend_id = src.trend_id
				  AND tgt.date_time_local_15min = src.date_time_local_15min
				 )
	  WHEN MATCHED 
	  	   AND 	IFNULL(tgt.median_value_15min,0) != IFNULL(src.median_value_15min,0)
	  	   AND  IFNULL(tgt.max_value_15min,0) != IFNULL(src.max_value_15min,0)
	  THEN
		UPDATE 
		SET
				tgt.median_value_15min = src.median_value_15min,
				tgt.max_value_15min = src.max_value_15min,
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
				src.is_business_hours,
				src.median_value_15min,
				src.max_value_15min,
				SYSDATE(), 
				:task_name,
				SYSDATE(),
				:task_name
		);
    $$
;