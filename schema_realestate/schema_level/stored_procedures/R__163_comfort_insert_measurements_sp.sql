-- ******************************************************************************************************************************
-- Stored procedure that incrementally populates the comfort_measurements table
-- ******************************************************************************************************************************
CREATE OR REPLACE PROCEDURE transformed.comfort_insert_measurements_sp(task_name VARCHAR)
    RETURNS STRING
    LANGUAGE SQL
  AS
    $$
      BEGIN

        CREATE OR REPLACE TEMPORARY TABLE watermark AS 
          SELECT IFNULL(MIN(timestamp_utc),'2019-01-01') AS min_date
          FROM transformed.comfort_transient_telemetry
        ;

        CREATE OR REPLACE TEMPORARY TABLE transformed.comfort_assets_unique AS
        SELECT DISTINCT
            asset_id,
            setpoint_trend_id,
            setpoint_model_id,
            unit,
            sensor_trend_id
        FROM transformed.comfort_assets;

        CREATE OR REPLACE TEMPORARY TABLE transformed.temporary_comfort_measurements AS
        SELECT DISTINCT 
            assets.asset_id,
            assets.sensor_trend_id,
            assets.setpoint_trend_id,
            ts.timestamp_utc AS captured_at,
            assets.unit, 
            ts.telemetry_value,
            '' -- :task_name
            AS _created_by_task,
            ts.enqueued_at AS enqueued_at_utc
        FROM transformed.comfort_transient_telemetry ts
            JOIN  transformed.comfort_assets_unique assets
              ON (assets.sensor_trend_id = ts.trend_id)
        WHERE
               ( (assets.unit = 'degC' AND ts.telemetry_value >= 5 AND ts.telemetry_value <= 50)
           OR  (assets.unit = 'degF' AND ts.telemetry_value >= 32 AND ts.telemetry_value <= 120) )
        ;

        CREATE OR REPLACE TEMPORARY TABLE eff_cooling_sp AS
        -- Priority 1 Effective or Unique Cooling Setpoints:
          SELECT
              sp.asset_id, 
              sp.external_id,
              sp.trend_id,
              'Effective heating/cooling setpoints' AS sp_type,
              (CASE WHEN sp.setpoint_model_id ilike 'dtmi:com:willowinc:%Cooling%' THEN sp.setpoint_value ELSE NULL END)  AS max_setpoint_value,
              sp._valid_from AS valid_from, 
              sp._valid_to AS valid_to
          FROM transformed.comfort_setpoints sp
          WHERE sp.setpoint_model_id IN ('dtmi:com:willowinc:EffectiveCoolingZoneAirTemperatureSetpoint;1','dtmi:com:willowinc:CoolingZoneAirTemperatureSetpoint;1')
          AND sp._valid_to >= (DATEADD('d',-1,COALESCE((SELECT min_date FROM watermark),'2018-01-01')))
        ;
        -- Priority 1 Effective or Unique Heating Setpoints:
        CREATE OR REPLACE TEMPORARY TABLE eff_heating_sp AS
          SELECT  
              sp.asset_id, 
              sp.trend_id,
              'Effective heating/cooling setpoints' AS sp_type,
              (CASE WHEN setpoint_model_id ilike 'dtmi:com:willowinc:%heating%' THEN sp.setpoint_value ELSE NULL END)  AS min_setpoint_value,
              sp._valid_from AS valid_from, 
              sp._valid_to AS valid_to
          FROM transformed.comfort_setpoints sp
          WHERE setpoint_model_id IN ('dtmi:com:willowinc:EffectiveHeatingZoneAirTemperatureSetpoint;1','dtmi:com:willowinc:HeatingZoneAirTemperatureSetpoint;1')
          AND sp._valid_to >= DATEADD('d',-1,COALESCE((SELECT min_date FROM watermark),'2018-01-01'))
        ;

        -- create table for effective setpoints
        CREATE OR REPLACE TRANSIENT TABLE transformed.transient_effective_heat_cool AS
          SELECT DISTINCT
            measurements.asset_id,
            measurements.sensor_trend_id,
            measurements.captured_at,
            measurements.unit, 
            COALESCE(eff_cooling_sp.sp_type, eff_heating_sp.sp_type) AS setpoint_type,
            MAX(measurements.telemetry_value) AS zone_air_temp,
            MAX(eff_heating_sp.min_setpoint_value) AS min_setpoint_value,
            MAX(eff_cooling_sp.max_setpoint_value) AS max_setpoint_value,
            _created_by_task,
            MAX(measurements.enqueued_at_utc) AS enqueued_at_utc
          FROM transformed.temporary_comfort_measurements measurements
            LEFT JOIN eff_cooling_sp	  
               ON measurements.setpoint_trend_id = eff_cooling_sp.trend_id
              AND measurements.asset_id = eff_cooling_sp.asset_id
              AND measurements.captured_at BETWEEN eff_cooling_sp.valid_from AND eff_cooling_sp.valid_to
            LEFT JOIN eff_heating_sp  
               ON measurements.setpoint_trend_id = eff_heating_sp.trend_id
              AND measurements.asset_id = eff_heating_sp.asset_id
              AND measurements.captured_at BETWEEN eff_heating_sp.valid_from AND eff_heating_sp.valid_to
            WHERE COALESCE(eff_cooling_sp.sp_type, eff_heating_sp.sp_type) IS NOT NULL
        GROUP BY
            measurements.sensor_trend_id,
            measurements.captured_at,
            measurements.asset_id,
            measurements.unit,
            setpoint_type,
            _created_by_task
        ;
          INSERT INTO transformed.comfort_measurements (
              asset_id, sensor_trend_id, captured_at, unit, setpoint_type, zone_air_temp, 
              min_setpoint_value, max_setpoint_value, _created_by_task, last_enqueued_at
          )
          SELECT 
              asset_id, sensor_trend_id, captured_at, unit, setpoint_type, zone_air_temp, 
              min_setpoint_value, max_setpoint_value, _created_by_task, enqueued_at_utc
          FROM transformed.transient_effective_heat_cool;
----------------------------------------------------------------------------------------------------------
        -- PRIORITY 2 & 3: Occupied AND Unoccupied Cooling Setpoints
        CREATE OR REPLACE TEMPORARY TABLE occupied_cooling AS
            SELECT 
                occ.asset_id,
                occ.occupancy_model_id, 
                sp.setpoint_model_id,
                sp.trend_id,
                'Occupied heating/cooling setpoints' AS sp_type,
                occ.occupancy_value, 
                occ._valid_from AS occ_valid_from,     
                occ._valid_to AS occ_valid_to,  
                sp.setpoint_unit, 
                sp._valid_from AS sp_valid_from, 
                sp._valid_to AS sp_valid_to,
                (CASE   WHEN sp.setpoint_model_id ilike 'dtmi:com:willowinc:Occup%Cooling%' AND occ.occupancy_value >= 1 
                                THEN sp.setpoint_value 
                            WHEN sp.setpoint_model_id ilike 'dtmi:com:willowinc:UnoccupiedCooling%'
                                THEN sp.setpoint_value
                            WHEN sp.setpoint_model_id ilike 'dtmi:com:willowinc:Occup%Cooling%'
                                THEN sp.setpoint_value
                          ELSE NULL END)  AS max_setpoint_value
            FROM transformed.comfort_occupancy occ
            JOIN transformed.comfort_setpoints sp 
                 ON (occ._valid_from BETWEEN sp._valid_from AND sp._valid_to
                 OR   sp._valid_from BETWEEN occ._valid_from AND occ._valid_to)
                AND occ.asset_id = sp.asset_id
                AND sp.setpoint_model_id ilike 'dtmi:com:willowinc:%Occup%Cooling%'
                AND occ._valid_to >= DATEADD('d',-1,COALESCE((SELECT min_date FROM watermark),'2018-01-01'))
                AND  sp._valid_to >= DATEADD('d',-1,COALESCE((SELECT min_date FROM watermark),'2018-01-01'))
            WHERE
                NOT EXISTS (SELECT 1 FROM transformed.transient_effective_heat_cool eff_sp WHERE eff_sp.asset_id = sp.asset_id)
        ;
        -- PRIORITY 2 & 3: Occupied AND Unoccupied heating Setpoints
        CREATE OR REPLACE TEMPORARY TABLE occupied_heating AS
            SELECT 
                occ.asset_id,
                occ.occupancy_model_id, 
                sp.setpoint_model_id,
                sp.trend_id,
                'Occupied heating/cooling setpoints' AS sp_type,
                occ.occupancy_value, 
                occ._valid_from AS occ_valid_from,     
                occ._valid_to AS occ_valid_to,  
                sp.setpoint_unit, 
                sp._valid_from AS sp_valid_from, 
                sp._valid_to AS sp_valid_to,
                (CASE   WHEN sp.setpoint_model_id ilike 'dtmi:com:willowinc:Occup%Heating%' AND occ.occupancy_value >= 1 
                                THEN sp.setpoint_value 
                            WHEN sp.setpoint_model_id ilike 'dtmi:com:willowinc:Unoccupiedheating%'
                                THEN sp.setpoint_value
                            WHEN sp.setpoint_model_id ilike 'dtmi:com:willowinc:Occup%Heating%'
                                THEN sp.setpoint_value
                          ELSE NULL END)  AS min_setpoint_value
            FROM transformed.comfort_occupancy occ
            JOIN transformed.comfort_setpoints sp 
                 ON (occ._valid_from BETWEEN sp._valid_from AND sp._valid_to
                 OR   sp._valid_from BETWEEN occ._valid_from AND occ._valid_to)
                AND occ.asset_id = sp.asset_id
                AND sp.setpoint_model_id ilike 'dtmi:com:willowinc:%Occup%Heating%'
                AND occ._valid_to >= DATEADD('d',-1,COALESCE((SELECT min_date FROM watermark),'2018-01-01'))
                AND  sp._valid_to >= DATEADD('d',-1,COALESCE((SELECT min_date FROM watermark),'2018-01-01'))
            WHERE
                NOT EXISTS (SELECT 1 FROM eff_cooling_sp eff_sp WHERE eff_sp.asset_id = sp.asset_id)
        ;
        -- Create table for occupied setpoints
        CREATE OR REPLACE TABLE transformed.transient_occupancy_heat_cool AS
         SELECT 
            measurements.asset_id,
            measurements.sensor_trend_id,
            measurements.captured_at,
            measurements.unit, 
            COALESCE(occ_cooling.sp_type, occ_heating.sp_type) AS setpoint_type,
            MAX(measurements.telemetry_value) AS zone_air_temp,
            MAX(occ_heating.min_setpoint_value) AS min_setpoint_value,
            MAX(occ_cooling.max_setpoint_value) AS max_setpoint_value,
            _created_by_task,
            MAX(measurements.enqueued_at_utc) AS enqueued_at_utc
          FROM transformed.temporary_comfort_measurements measurements
             LEFT JOIN occupied_cooling occ_cooling
               ON measurements.setpoint_trend_id = occ_cooling.trend_id
              AND measurements.captured_at BETWEEN occ_cooling.sp_valid_from AND occ_cooling.sp_valid_to
              AND measurements.captured_at BETWEEN occ_cooling.occ_valid_from AND occ_cooling.occ_valid_to
              AND measurements.asset_id = occ_cooling.asset_id
             LEFT JOIN occupied_heating occ_heating
               ON measurements.setpoint_trend_id = occ_heating.trend_id
              AND measurements.captured_at BETWEEN occ_heating.sp_valid_from AND occ_heating.sp_valid_to
              AND measurements.captured_at BETWEEN occ_heating.occ_valid_from AND occ_heating.occ_valid_to
              AND measurements.asset_id = occ_heating.asset_id
             WHERE setpoint_type is not null
              AND occ_heating.min_setpoint_value IS NOT NULL
              AND occ_cooling.max_setpoint_value IS NOT NULL
        GROUP BY
            measurements.sensor_trend_id,
            measurements.captured_at,
            measurements.asset_id,
            measurements.unit,
            setpoint_type,
            _created_by_task
        ;
          INSERT INTO transformed.comfort_measurements (
              asset_id, sensor_trend_id, captured_at, unit, setpoint_type, zone_air_temp, 
              min_setpoint_value, max_setpoint_value, _created_by_task, last_enqueued_at
          )
          SELECT
              asset_id, sensor_trend_id, captured_at, unit, setpoint_type, zone_air_temp, 
              min_setpoint_value, max_setpoint_value, _created_by_task, enqueued_at_utc
          FROM transformed.transient_occupancy_heat_cool;

        -- PRIORITY 4: Singular Setpoints
        CREATE OR REPLACE TEMPORARY TABLE singular_setpoints AS
          SELECT 
          sp.asset_id, 
          sp.trend_id,
          'Singular setpoint' AS sp_type, 
          setpoint_value, 
          setpoint_value AS min_setpoint_value, 
          setpoint_value AS max_setpoint_value, 
          _valid_from, 
          _valid_to
          FROM transformed.comfort_setpoints sp
          LEFT JOIN (SELECT asset_id, MIN(_valid_from) AS valid_from , MAX(_valid_to) AS valid_to
                        FROM transformed.comfort_setpoints sp
                        WHERE EXISTS (SELECT 1 FROM transformed.transient_effective_heat_cool eff WHERE sp.asset_id = eff.asset_id) 
                           OR EXISTS (SELECT 1 FROM transformed.transient_occupancy_heat_cool occ WHERE sp.asset_id = occ.asset_id) 
                        GROUP By asset_id
                        ) eff
                  ON sp.asset_id = eff.asset_id
                  AND sp._valid_from BETWEEN eff.valid_from AND eff.valid_to
          WHERE sp.setpoint_model_id IN (
                  'dtmi:com:willowinc:EffectiveZoneAirTemperatureSetpoint;1',
                  'dtmi:com:willowinc:ActiveZoneAirTemperatureSetpoint;1',
                  'dtmi:com:willowinc:AirTemperatureSetpoint;1',
                  'dtmi:com:willowinc:ZoneAirTemperatureSetpoint;1',
                  'dtmi:com:willowinc:TargetZoneAirTemperatureSetpoint;1'
                  )
                AND sp._valid_to >= DATEADD('d',-1,COALESCE((SELECT min_date FROM watermark),'2018-01-01'))
                AND eff.asset_id IS NULL
        ;
        -- Create table for singular setpoints
        CREATE OR REPLACE TABLE transformed.transient_singular_setpoints AS
          SELECT 
            measurements.asset_id,
            measurements.sensor_trend_id,
            measurements.captured_at,
            measurements.unit, 
            sp_singular.sp_type AS setpoint_type,
            MAX(measurements.telemetry_value) AS zone_air_temp,
            MAX(sp_singular.min_setpoint_value) AS min_setpoint_value,
            MAX(sp_singular.max_setpoint_value) AS max_setpoint_value,
            _created_by_task,
            MAX(measurements.enqueued_at_utc) AS enqueued_at_utc
          FROM transformed.temporary_comfort_measurements measurements
            JOIN singular_setpoints sp_singular 
               ON measurements.setpoint_trend_id = sp_singular.trend_id
              AND measurements.captured_at BETWEEN sp_singular._valid_from AND sp_singular._valid_to
              AND measurements.asset_id = sp_singular.asset_id
        GROUP BY
            measurements.sensor_trend_id,
            measurements.captured_at,
            measurements.asset_id,
            measurements.unit,
            setpoint_type,
            _created_by_task
        ;
          INSERT INTO transformed.comfort_measurements (
              asset_id, sensor_trend_id, captured_at, unit, setpoint_type, zone_air_temp, 
              min_setpoint_value, max_setpoint_value, _created_by_task, last_enqueued_at
          ) 
          SELECT
              asset_id, sensor_trend_id, captured_at, unit, setpoint_type, zone_air_temp, 
              min_setpoint_value, max_setpoint_value, _created_by_task, enqueued_at_utc
          FROM transformed.transient_singular_setpoints;
        ;


          CREATE OR REPLACE TABLE transformed.transient_setpoints AS
          SELECT * FROM transformed.transient_effective_heat_cool
          UNION ALL
          SELECT * FROM transformed.transient_occupancy_heat_cool
          UNION ALL
          SELECT * FROM transformed.transient_singular_setpoints;
      END;
    $$
;
