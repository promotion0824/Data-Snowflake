-- ******************************************************************************************************************************
-- Stored procedure that incrementally populates the chiller_15mins table
-- ******************************************************************************************************************************

CREATE OR REPLACE PROCEDURE transformed.merge_chiller_15min_sp(task_name VARCHAR)
    RETURNS STRING
    LANGUAGE SQL
  AS
    $$
      BEGIN
        LET watermark TIMESTAMP_NTZ := (SELECT MAX(date_time_local_15min) FROM transformed.chiller_15mins);
               
        MERGE INTO transformed.chiller_15mins AS tgt 
          USING (
            SELECT
              scores.asset_id,
              scores.site_id,
              DATE(date_time_local_15min) AS date_local,
              date_time_local_15min,
              scores.unit,
              scores.sensor_type,
              AVG(CASE WHEN chiller_run_status=TRUE THEN chiller_delta_temp ELSE NULL END) AS avg_chiller_delta_temp,
              COUNT_IF(chiller_run_status) AS count_run_sensor_on,
              COUNT_IF(chiller_run_status=FALSE) AS count_run_sensor_off
            FROM transformed.chiller_delta_measurements scores             
              JOIN transformed.site_defaults working_hours
                ON (scores.site_id = working_hours.site_id) 
               AND (working_hours.type = 'WorkingHours')
               AND (working_hours._valid_from <= TIME_SLICE(date_time_local_15min, 1, 'HOUR') AND working_hours._valid_to >= TIME_SLICE(date_time_local_15min, 1, 'HOUR'))
            WHERE captured_at >= IFNULL(:watermark, TO_TIMESTAMP('0000-01-01')) 
            GROUP BY 
              scores.asset_id, 
              scores.site_id,
              DATE(date_time_local_15min),
              date_time_local_15min, 
              scores.unit,
              scores.sensor_type
          ) AS src
            ON (tgt.asset_id = src.asset_id AND tgt.date_time_local_15min = src.date_time_local_15min)
            
          WHEN MATCHED THEN
          
            UPDATE 
            SET
              tgt.unit = src.unit,
              tgt.sensor_type = src.sensor_type,
              tgt.avg_chiller_delta_temp = src.avg_chiller_delta_temp,
              tgt.count_run_sensor_on = src.count_run_sensor_on,
              tgt.count_run_sensor_off = src.count_run_sensor_off,
              tgt._last_updated_at = SYSDATE(),
              tgt._last_updated_by_task = :task_name      
          WHEN NOT MATCHED THEN
          
            INSERT (
              asset_id, 
              site_id,
              date_local, 
              date_time_local_15min,
              unit,
              sensor_type,
              avg_chiller_delta_temp,
              count_run_sensor_on,
              count_run_sensor_off,
              _created_by_task,
              _last_updated_by_task
              ) 
            VALUES (
              src.asset_id, 
              src.site_id,
              src.date_local,
              src.date_time_local_15min, 
              src.unit,
              src.sensor_type,
              src.avg_chiller_delta_temp,
              src.count_run_sensor_on,
              src.count_run_sensor_off,
              :task_name,
              :task_name
            );
			    EXCEPTION
          WHEN statement_error THEN
            RETURN OBJECT_CONSTRUCT('Error type', 'STATEMENT_ERROR',
                                    'SQLCODE', sqlcode,
                                    'SQLERRM', sqlerrm,
                                    'SQLSTATE', sqlstate);
      END;		
    $$
;