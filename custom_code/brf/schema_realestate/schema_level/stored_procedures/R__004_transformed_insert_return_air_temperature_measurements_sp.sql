-- ******************************************************************************************************************************
-- Stored procedure that incrementally populates the air_temperature_measurements table
-- ******************************************************************************************************************************

CREATE OR REPLACE PROCEDURE transformed.insert_return_air_temperature_measurements_sp(task_name VARCHAR)
    RETURNS STRING
    LANGUAGE SQL
  AS
    $$
      BEGIN
        LET watermark TIMESTAMP_NTZ := (SELECT MAX(captured_at) FROM transformed.return_air_temperature_measurements);

        INSERT INTO transformed.return_air_temperature_measurements (
          asset_twin_id, site_id, sensor_trend_id, setpoint_trend_id, captured_at, 
		  return_air_temperature, return_air_temperature_sp, deviation, return_air_humidity,
		  _created_by_task)  
          SELECT
            assets.asset_id,
            assets.site_id,
            assets.sensor_trend_id,
            assets.setpoint_trend_id,
            measurements.timestamp_utc AS captured_at,
            CASE WHEN assets.model_id = 'dtmi:com:willowinc:ReturnAirTemperatureSensor;1' 
                THEN measurements.telemetry_value 
            ELSE NULL 
            END AS return_air_temperature,
            CASE WHEN assets.model_id = 'dtmi:com:willowinc:ReturnAirTemperatureSensor;1' 
                THEN IFNULL(setpoints.return_air_temperature_sp, TRY_CAST(default_setpoints.default_value:value::string AS DOUBLE)) 
            ELSE NULL 
            END AS return_air_temperature_sp,
            CASE WHEN assets.model_id = 'dtmi:com:willowinc:ReturnAirTemperatureSensor;1' 
                THEN IFNULL(setpoints.return_air_temperature_sp, TRY_CAST(default_setpoints.default_value:value::string AS DOUBLE)) - return_air_temperature 
            ELSE NULL 
            END AS deviation,
            CASE WHEN assets.model_id = 'dtmi:com:willowinc:ReturnAirHumiditySensor;1' 
                THEN measurements.telemetry_value ELSE NULL 
            END AS return_air_humidity,
            :task_name 
          FROM transformed.return_air_temperature_assets assets
          JOIN transformed.time_series_enriched measurements 
            ON (assets.site_id = measurements.site_id)
			     AND (assets.sensor_trend_id = measurements.trend_id)
          LEFT JOIN transformed.site_defaults default_setpoints
            ON (
                assets.site_id = default_setpoints.site_id 
            AND default_setpoints.type = 'ReturnAirTemperatureSetpointDefault' 
            AND default_setpoints.default_value:unit::STRING = assets.unit
            AND default_setpoints._valid_from <= measurements.timestamp_utc 
            AND default_setpoints._valid_to >= measurements.timestamp_utc
              )
          LEFT JOIN transformed.return_air_temperature_setpoints setpoints
             ON (
                assets.site_id = setpoints.site_id
            AND assets.asset_id = setpoints.asset_twin_id		  
            AND measurements.timestamp_utc BETWEEN setpoints._valid_from AND setpoints._valid_to
            AND assets.setpoint_trend_id = setpoints.trend_id
             )
          WHERE measurements.timestamp_utc > IFNULL(:watermark, TO_TIMESTAMP('0000-01-01'));          
      END;
    $$
;