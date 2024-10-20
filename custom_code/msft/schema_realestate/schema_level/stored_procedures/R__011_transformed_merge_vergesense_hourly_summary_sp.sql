-- ******************************************************************************************************************************
-- Stored procedure that incrementally populates the zone_air_temp_hourly_metrics table
-- ******************************************************************************************************************************

CREATE OR REPLACE PROCEDURE transformed.merge_vergesense_hourly_sp()
    RETURNS STRING
    LANGUAGE SQL
  AS
    $$
      BEGIN
        
        MERGE INTO transformed.vergesense_hourly_summary AS tgt 
        
          USING (
			WITH watermark AS 
			(
			  SELECT DISTINCT
					IFNULL(MAX(_last_updated_at),'2022-01-01') AS max_updated
			  FROM transformed.vergesense_hourly_summary
			)
			SELECT 
				ts.date_local,
				ts.start_of_hour,
				ts.trend_id,
				ts.capability_name,
				CASE WHEN ts.model_id = 'dtmi:com:willowinc:PeopleCountSensor;1' THEN AVG(ts.analog_value) ELSE NULL END AS average,
				CASE WHEN ts.model_id = 'dtmi:com:willowinc:PeopleCountSensor;1' THEN MIN(ts.analog_value) ELSE NULL END AS minimum,
				CASE WHEN ts.model_id = 'dtmi:com:willowinc:PeopleCountSensor;1' THEN MAX(ts.analog_value) ELSE NULL END AS maximum,
				CASE WHEN ts.model_id = 'dtmi:com:willowinc:PeopleCountSensor;1' THEN 'analog' ELSE 'digital' END AS type,
				SUM(off_count) AS off_count,
				SUM(on_count) AS on_count,
				ts.model_id,
				ts.asset_id,
				ts.site_id
			FROM transformed.vergesense_time_series ts
		   WHERE ts._last_updated_at >= (SELECT max_updated FROM watermark)
			  OR ts._last_updated_at IS NULL
			GROUP BY 
				ts.date_local,
				ts.start_of_hour,
				ts.trend_id, 
				ts.capability_name,
				ts.model_id,
				ts.asset_id,
				ts.site_id
          ) AS src
            ON (tgt.start_of_hour = src.start_of_hour AND tgt.site_id = src.site_id AND tgt.trend_id = src.trend_id AND tgt.asset_id = src.asset_id)
            
          WHEN MATCHED THEN
          
            UPDATE 
            SET
              tgt.date_local = src.date_local,
              tgt.capability_name = src.capability_name,
              tgt.average = src.average,
              tgt.minimum = src.minimum,
              tgt.maximum = src.maximum,
              tgt.type = src.type,
              tgt.off_count = src.off_count,
              tgt.on_count = src.on_count,
              tgt.model_id = src.model_id,
              tgt.asset_id = src.asset_id,		  		  		  
              tgt._last_updated_at = SYSDATE()             
 
 WHEN NOT MATCHED THEN
          
            INSERT (
				date_local,
				start_of_hour,
				trend_id,
				capability_name,
				average,
				minimum,
				maximum,
				type,
				off_count,
				on_count,
				model_id,
				asset_id,
				site_id,
				_last_updated_at
			  ) 
            VALUES (
				src.date_local,
				src.start_of_hour,
				src.trend_id,
				src.capability_name,
				src.average,
				src.minimum,
				src.maximum,
				src.type,
				src.off_count,
				src.on_count,
				src.model_id,
				src.asset_id,
				src.site_id,
				SYSDATE()
            );

      END;
    $$
;