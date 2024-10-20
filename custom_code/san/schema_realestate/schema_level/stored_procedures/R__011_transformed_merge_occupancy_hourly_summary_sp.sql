-- ******************************************************************************************************************************
-- Stored procedure that incrementally populates the zone_air_temp_hourly_metrics table
-- ******************************************************************************************************************************

CREATE OR REPLACE PROCEDURE transformed.merge_occupancy_hourly_sp()
    RETURNS STRING
    LANGUAGE SQL
  AS
    $$
      BEGIN
        
        MERGE INTO transformed.occupancy_hourly_summary AS tgt 
        
          USING (
			WITH watermark AS 
			(
			  SELECT DISTINCT
					IFNULL(MAX(_last_updated_at),'2022-01-01') AS max_updated
			  FROM transformed.occupancy_hourly_summary
			)
			SELECT 
				ts.date_local,
				ts.date_time_local_hour,
				ts.trend_id,
				ts.capability_name,
				SUM(ts.telemetry_value) AS sum,
				AVG(ts.telemetry_value) AS average,
				MIN(ts.telemetry_value) AS minimum,
				MAX(ts.telemetry_value) AS maximum,
				MAX(last_value_hour) AS last_value_hour,
				COUNT(ts.telemetry_value) AS count,
				ts.model_id,
				ts.asset_id,
				ts.site_id
			FROM transformed.occupancy_time_series ts
		   WHERE ts._last_updated_at >= (SELECT max_updated FROM watermark)
			  OR ts._last_updated_at IS NULL
			GROUP BY 
				ts.date_local,
				ts.date_time_local_hour,
				ts.trend_id, 
				ts.capability_name,
				ts.model_id,
				ts.asset_id,
				ts.site_id
          ) AS src
            ON (tgt.date_time_local_hour = src.date_time_local_hour AND tgt.trend_id = src.trend_id)
            
          WHEN MATCHED THEN
          
            UPDATE 
            SET
              tgt.date_local = src.date_local,
			  tgt.date_time_local_hour = src.date_time_local_hour,
              tgt.capability_name = src.capability_name,
			  tgt.sum = src.sum,
              tgt.average = src.average,
              tgt.minimum = src.minimum,
              tgt.maximum = src.maximum,
              tgt.last_value_hour = src.last_value_hour,
              tgt.count = src.count,
              tgt.model_id = src.model_id,
              tgt.asset_id = src.asset_id,		  		  		  
              tgt._last_updated_at = SYSDATE()             
 
 WHEN NOT MATCHED THEN
          
            INSERT (
				date_local,
				date_time_local_hour,
				trend_id,
				capability_name,
				sum,
				average,
				minimum,
				maximum,
				last_value_hour,
				count,
				model_id,
				asset_id,
				site_id,
				_last_updated_at
			  ) 
            VALUES (
				src.date_local,
				src.date_time_local_hour,
				src.trend_id,
				src.capability_name,
				src.sum,
				src.average,
				src.minimum,
				src.maximum,
				src.last_value_hour,
				src.count,
				src.model_id,
				src.asset_id,
				src.site_id,
				SYSDATE()
            );

      END;
    $$
;