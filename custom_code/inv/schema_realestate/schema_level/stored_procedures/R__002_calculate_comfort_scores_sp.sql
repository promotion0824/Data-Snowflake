-- ------------------------------------------------------------------------------------------------------------------------------
-- Stored procedure that calculates daily comfort score for each site
-- ------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE utils.calculate_comfort_scores_sp(date_from DATE, date_to DATE, task_name STRING)
    RETURNS STRING
    LANGUAGE JAVASCRIPT
    STRICT
    EXECUTE AS OWNER
  AS
    $$
    var dateFrom = DATE_FROM.toISOString();
    var dateTo = DATE_TO.toISOString();
    var taskName = (TASK_NAME == null) ? 'unknown' : TASK_NAME;

    var sqlCommand = `
      MERGE INTO transformed.site_daily_comfort_scores AS tgt 
        USING (
          WITH cte_temp_measurements AS (

              SELECT
                  tsm.site_id,
                  captured_at_local,
                  CASE 
                    WHEN tsm.measurement_value_degc >= t_low.threshold_value AND tsm.measurement_value_degc <= t_high.threshold_value THEN true
                    ELSE false
                  END AS is_in_acceptable_range
                FROM transformed.zone_air_temperature_sensor_measurements tsm
                  JOIN utils.dates d 
                    ON (TO_DATE(tsm.captured_at_local) = d.date AND d.is_weekday = true)  
                  JOIN transformed.site_thresholds t_low 
                    ON (tsm.site_id = t_low.site_id AND t_low.type = 'ZoneAirTemperatureDegC_Low' AND tsm.captured_at_local BETWEEN t_low._valid_from AND t_low._valid_to)
                  JOIN transformed.site_thresholds t_high 
                    ON (tsm.site_id = t_high.site_id AND t_high.type = 'ZoneAirTemperatureDegC_High' AND tsm.captured_at_local BETWEEN t_high._valid_from AND t_high._valid_to)  
                -- TODO: This should come from site configuration
                WHERE 
                  TO_DATE(tsm.captured_at_local) BETWEEN :1 AND :2
                  AND TO_TIME(tsm.captured_at_local) >= TO_TIME('08:00:00') AND TO_TIME(tsm.captured_at_local) <= TO_TIME('18:00:00')

          )

            SELECT 
              site_id, 
              TO_DATE(captured_at_local) AS date,
              COUNT(1) AS measurements_count,
              SUM(CASE WHEN is_in_acceptable_range THEN 1 ELSE 0 END) AS measurements_in_range_count,
              CAST(measurements_in_range_count / measurements_count AS NUMERIC(3,2)) AS score_value
            FROM cte_temp_measurements
            GROUP BY 
              site_id, 
              date
        ) AS src
          ON (tgt.site_id = src.site_id AND tgt.date = src.date)
        WHEN MATCHED THEN
          UPDATE 
          SET 
            tgt.measurements_count = src.measurements_count,
            tgt.measurements_in_range_count = src.measurements_in_range_count,
            tgt.score_value = src.score_value,
            tgt._last_updated_at = SYSDATE(),
            tgt._last_updated_by_task = :3
        WHEN NOT MATCHED THEN
          INSERT (
            site_id, 
            date, 
            measurements_count, 
            measurements_in_range_count,
            score_value,
            _created_at,
            _created_by_task,
            _last_updated_at,
            _last_updated_by_task) 
          VALUES (
            src.site_id, 
            src.date,
            src.measurements_count, 
            src.measurements_in_range_count, 
            src.score_value,
            SYSDATE(), 
            :3,
            SYSDATE(),
            :3
          )         
     `;
     
    try {
        snowflake.execute (
            {
                sqlText: sqlCommand, 
                binds: [dateFrom, dateTo, taskName]}
            );
        return 'Succeeded.'; 
        }
    catch (err)  {
        return "Failed: " + err;
        throw err;
        }
    $$
;