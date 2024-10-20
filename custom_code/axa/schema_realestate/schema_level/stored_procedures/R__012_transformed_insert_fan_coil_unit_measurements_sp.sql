-- ******************************************************************************************************************************
-- Stored procedure that incrementally populates the fan_coil_unit table
-- ******************************************************************************************************************************
CREATE OR REPLACE PROCEDURE transformed.insert_fan_coil_unit_measurements_sp(task_name VARCHAR)
    RETURNS STRING
    LANGUAGE SQL
  AS
    $$
      BEGIN
        LET watermark TIMESTAMP_NTZ := (SELECT MAX(max_timestamp_local) FROM transformed.fan_coil_unit_measurements);

        CREATE OR REPLACE TRANSIENT TABLE transformed.fan_coil_unit_assets AS SELECT * FROM transformed.fan_coil_unit_assets_v;

        CREATE OR REPLACE TEMPORARY TABLE measurements AS
        SELECT DISTINCT
            assets.asset_id,
            assets.site_id,
            assets.trend_id,
            assets.model_id,
            assets.capability_name,
            ts.timestamp_local,
            ts.date_local,
            ts.date_time_local_15min,
            assets.unit, 
            ts.telemetry_value,
            (CASE WHEN model_id IN ('dtmi:com:willowinc:ModeSensor;1','dtmi:com:willowinc:ModeState;1') AND assets.capability_name = 'Occupation Synthèse (1=Occupé ; 2=Inoccupé ; 4=Standby)' THEN 
                FIRST_VALUE(IFF(Unit='Mode', ts.telemetry_value, null) IGNORE NULLS) OVER (PARTITION BY assets.asset_id,assets.trend_id,ts.date_time_local_15min ORDER BY ts.timestamp_local NULLS LAST)
            ELSE NULL END) AS mode_sensor,            
            (CASE WHEN model_id = 'dtmi:com:willowinc:ZoneAirTemperatureSensor;1' THEN telemetry_value ELSE NULL END) AS zone_air_temperature,
            :task_name AS _created_by_task,
            ts.enqueued_at AS enqueued_at_utc
        FROM transformed.fan_coil_unit_assets assets
            JOIN transformed.time_series_enriched ts 
              ON (assets.trend_id = ts.trend_id) 
        WHERE ts.timestamp_local > IFNULL(:watermark, TO_TIMESTAMP('0000-01-01'))
          AND (assets.model_id = 'dtmi:com:willowinc:ZoneAirTemperatureSensor;1' OR assets.capability_name = 'Occupation Synthèse (1=Occupé ; 2=Inoccupé ; 4=Standby)')
        ;

        INSERT INTO transformed.fan_coil_unit_measurements (
          asset_id, site_id, date_local, date_time_local_15min, mode_sensor,zone_air_temperature,sample_count,
          _created_by_task, max_timestamp_local, max_enqueued_at_utc) 
         SELECT
            measurements.asset_id,
            measurements.site_id,
            measurements.date_local,
            measurements.date_time_local_15min,
            MIN(mode_sensor) AS mode_sensor,
            AVG(zone_air_temperature) AS zone_air_temperature,
            COUNT(telemetry_value) AS sample_count,
            :task_name AS _created_by_task,
            MAX(measurements.timestamp_local) AS max_timestamp_local,
            MAX(measurements.enqueued_at_utc) AS max_enqueued_at_utc
        FROM measurements
        GROUP BY
            measurements.asset_id,
            measurements.site_id,
            measurements.date_local,
            measurements.date_time_local_15min
        ;
      END;		
    $$
;