-- ******************************************************************************************************************************
-- Stored procedure that incrementally populates the chiller_efficiency_measurements table
-- ******************************************************************************************************************************
CREATE OR REPLACE PROCEDURE transformed.insert_chiller_efficiency_measurements_sp(task_name VARCHAR)
    RETURNS STRING
    LANGUAGE SQL
  AS
    $$
      BEGIN
        LET watermark TIMESTAMP_NTZ := (SELECT MAX(captured_at) FROM transformed.chiller_efficiency_measurements);

        CREATE OR REPLACE TEMPORARY TABLE measurements AS
        SELECT DISTINCT
            assets.asset_id,
            assets.site_id,
            assets.sensor_trend_id,
            assets.capability_name,
            assets.run_sensor_trend_id,
            ts.timestamp_utc AS captured_at,
            ts.date_time_local_15min,
            assets.unit, 
            ts.telemetry_value,
            :task_name AS _created_by_task,
            ts.enqueued_at AS enqueued_at_utc
        FROM transformed.chiller_efficiency_sensors assets
            JOIN transformed.time_series_enriched ts 
              ON (assets.site_id = ts.site_id)
			       AND (assets.sensor_trend_id = ts.trend_id) 
        WHERE ts.timestamp_utc > IFNULL(:watermark, TO_TIMESTAMP('0000-01-01'))
          AND ts.timestamp_utc >= '2023-03-15'
        ;

----------------------------------------------------------------------------------------------------------
        INSERT INTO transformed.chiller_efficiency_measurements (
          asset_id, site_id, trend_id, captured_at, date_time_local_15min, unit, sensor_value, chiller_run_status, _valid_from, _valid_to, sensor_type,
          _created_by_task, last_enqueued_at_utc) 
         SELECT
            measurements.asset_id,
            measurements.site_id,
            measurements.sensor_trend_id,
            measurements.captured_at,
            measurements.date_time_local_15min,
            measurements.unit, 
            AVG(CASE WHEN run_sensor.run_sensor_value >= 1 
                      THEN measurements.telemetry_value 
                      ELSE NULL END
               ) AS sensor_value,
            AVG(run_sensor.run_sensor_value) AS chiller_run_status,
            run_sensor._valid_from,
            run_sensor._valid_to,
            measurements.capability_name AS sensor_type,
            :task_name AS _created_by_task,
            MAX(measurements.enqueued_at_utc) AS enqueued_at_utc
        FROM measurements
          JOIN transformed.chiller_run_sensor run_sensor
               ON measurements.asset_id = run_sensor.asset_id
              AND measurements.captured_at BETWEEN run_sensor._valid_from AND run_sensor._valid_to
        GROUP BY
            measurements.asset_id,
            measurements.site_id, 
            measurements.sensor_trend_id,
            measurements.capability_name,
            measurements.captured_at,
            measurements.date_time_local_15min,
            run_sensor._valid_from,
            run_sensor._valid_to,
            measurements.unit
        ;
-----------------------------------------------------------------------------------------------------
        -- PRIORITY 4: no run_sensors
        CREATE OR REPLACE TEMPORARY TABLE no_run_sensors AS
          SELECT 
          sp.asset_id, 
          sp.sensor_trend_id,
          sp.site_id, 
          sp.capability_name AS sensor_type,
          telemetry_value AS sensor_value,
          captured_at,
          date_time_local_15min,
          unit,
          enqueued_at_utc
          FROM measurements sp
          LEFT JOIN (SELECT asset_id, MIN(_valid_from) AS valid_from , MAX(_valid_to) AS valid_to
                        FROM transformed.chiller_run_sensor
                        GROUP By asset_id
                        ) eff
                   ON sp.asset_id = eff.asset_id
                  AND sp.captured_at BETWEEN eff.valid_from AND eff.valid_to
          WHERE sp.captured_at >= IFNULL(:watermark, TO_TIMESTAMP('0000-01-01'))
                AND eff.asset_id IS NULL
        ;
        INSERT INTO transformed.chiller_efficiency_measurements (
          asset_id, site_id, trend_id, captured_at, date_time_local_15min, unit, sensor_type, sensor_value, 
          _created_by_task, last_enqueued_at_utc) 
          SELECT 
            rs.asset_id,
            rs.site_id,
            rs.sensor_trend_id,
            rs.captured_at,
            rs.date_time_local_15min,
            rs.unit, 
            rs.sensor_type,
            AVG(rs.sensor_value) AS sensor_value,
            :task_name AS _created_by_task,
            MAX(rs.enqueued_at_utc) AS enqueued_at_utc
          FROM no_run_sensors rs
        GROUP BY
            rs.asset_id,
            rs.site_id,
            rs.sensor_trend_id,
            rs.captured_at,
            rs.date_time_local_15min,
            rs.unit,
            rs.sensor_type
        ;
      END;
    $$
;