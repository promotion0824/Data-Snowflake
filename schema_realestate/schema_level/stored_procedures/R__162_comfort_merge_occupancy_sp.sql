-- ******************************************************************************************************************************
-- Stored procedure that incrementally populates the comfort occupancy table
-- ******************************************************************************************************************************

CREATE OR REPLACE PROCEDURE transformed.comfort_merge_occupancy_sp(task_name VARCHAR)
    RETURNS VARCHAR
    LANGUAGE SQL
  AS
  $$
    BEGIN
        CREATE OR REPLACE TEMPORARY TABLE watermark AS 
          SELECT IFNULL(MIN(timestamp_utc),'2019-01-01') AS min_date
          FROM transformed.comfort_transient_telemetry
        ;
        CREATE OR REPLACE TEMPORARY TABLE new_occupancy_values AS
          WITH cte_values_distinct AS (
            SELECT 
                assets.asset_id,
                assets.trend_id,
                assets.external_id,
                assets.model_id AS occupancy_model_id,
                assets.unit AS occupancy_unit,
                new_occupancy.timestamp_utc AS captured_at,
                telemetry_value,
                LAG(new_occupancy.telemetry_value) OVER (PARTITION BY assets.asset_id ORDER BY new_occupancy.timestamp_utc) AS previous_occ,
                IFF(previous_occ IS NULL, TO_TIMESTAMP_NTZ('0000-01-01 00:00:00.000'), new_occupancy.timestamp_utc) AS _valid_from,
                new_occupancy.telemetry_value AS occupancy_value,
                '' --:task_name 
                AS _created_by_task
            FROM transformed.comfort_transient_telemetry new_occupancy
            JOIN transformed.capabilities_assets assets 
              ON (new_occupancy.trend_id = assets.trend_id)
            WHERE  
                  model_id in (
                    'dtmi:com:willowinc:OccupiedActuator;1',
                    'dtmi:com:willowinc:OccupancySensor;1',
                    'dtmi:com:willowinc:OccupancySetpoint;1',
                    'dtmi:com:willowinc:OccupiedState;1'
                    )
              AND model_id_asset NOT IN ('dtmi:com:willowinc:Controller;1')
            ) 
      , cte_valid_from AS (
          WITH cte_valid_to AS (
                  SELECT 
                      asset_id,
                      trend_id,
                      external_id,
                      occupancy_model_id,
                      occupancy_unit,
                      captured_at,
                      LAG(new_occupancy.telemetry_value) OVER (PARTITION BY asset_id ORDER BY new_occupancy.captured_at) AS previous_occ,
                      IFF(previous_occ IS NULL, TO_TIMESTAMP_NTZ('0000-01-01 00:00:00.000'), new_occupancy.captured_at) AS _valid_from,
                      new_occupancy.telemetry_value AS occupancy_value,
                      _created_by_task
                  FROM cte_values_distinct new_occupancy
                  )
                  SELECT 
                    asset_id,
                    trend_id,
                    external_id,
                    occupancy_model_id,
                    occupancy_unit,
                    captured_at,
                    _valid_from,
                    IFNULL(DATEADD(ms, -1, LEAD(_valid_from) OVER (PARTITION BY asset_id ORDER BY captured_at)), TO_TIMESTAMP_NTZ('9999-12-31 23:59:59.999')) AS _valid_to,
                    occupancy_value,
                    _created_by_task
                  FROM cte_valid_to
      ) 
      SELECT * FROM cte_valid_from;

        -- If the first of new records has the same occupancy value as the last of the existing records set valid_to to valid_to of the new record 
        -- Update valid_to of the last existing record. If the occupancy_value is the same use valid_to of the new record, if it's different use valid_from of the new record - 1ms.
        UPDATE transformed.comfort_occupancy AS tgt
        SET 
          tgt._valid_to = CASE WHEN tgt.occupancy_value = src.occupancy_value THEN src._valid_to ELSE DATEADD(ms, -1, src.captured_at) END,
          tgt._last_updated_at = SYSDATE(),
          tgt._last_updated_by_task = src._created_by_task
        FROM new_occupancy_values AS src
        WHERE 
              tgt.trend_id = src.trend_id
          AND src.captured_at > tgt.first_captured_at
          AND tgt._valid_to = '9999-12-31 23:59:59.999' 
          AND src._valid_from = '0000-01-01 00:00:00.000'
          ;

        -- Insert the first new record from the new batch (if the value has changed)
        INSERT INTO transformed.comfort_occupancy (asset_id, trend_id, external_id, occupancy_model_id, occupancy_unit, first_captured_at,  _valid_from, _valid_to, occupancy_value, _created_by_task, _last_updated_by_task) 
          SELECT 
            src.asset_id,
            src.trend_id,
            src.external_id,
            src.occupancy_model_id,
            src.occupancy_unit,
            src.captured_at AS first_captured_at,
            src.captured_at AS _valid_from,
            src._valid_to,
            src.occupancy_value,
            src._created_by_task,
            src._created_by_task
          FROM new_occupancy_values AS src
          JOIN transformed.comfort_occupancy AS tgt 
		        ON (tgt.trend_id  = src.trend_id)  
          WHERE 
                tgt.asset_id  = src.asset_id
            AND src.captured_at > tgt.first_captured_at
            AND src._valid_from = '0000-01-01 00:00:00.000'
            AND src.captured_at = DATEADD(ms, 1, tgt._valid_to)
            AND IFNULL(tgt.occupancy_value,0) <> IFNULL(src.occupancy_value,0)
          ;

        -- Insert all subsequent new occupancys and completely new records (asset_id that has no previous occupancys) 
        INSERT INTO transformed.comfort_occupancy (asset_id, trend_id, external_id, occupancy_model_id, occupancy_unit, first_captured_at, _valid_from, _valid_to, occupancy_value, _created_by_task, _last_updated_by_task) 
          WITH cte_latest_current AS (
            SELECT DISTINCT
              asset_id,
              trend_id,
              external_id,
              occupancy_model_id,
              occupancy_unit,
              _valid_to
            FROM transformed.comfort_occupancy
            WHERE _valid_to >= (DATEADD('d',-2,COALESCE((SELECT min_date FROM watermark),'2018-01-01')))
            QUALIFY ROW_NUMBER() OVER (PARTITION BY asset_id ORDER BY _valid_to DESC) = 1

          )
          SELECT DISTINCT
            src.asset_id,
            src.trend_id,
            src.external_id,
            src.occupancy_model_id,
            src.occupancy_unit,
            src.captured_at,
            src._valid_from,
            src._valid_to,
            src.occupancy_value,
            src._created_by_task,
            src._created_by_task
          FROM new_occupancy_values AS src
            LEFT JOIN cte_latest_current AS tgt ON tgt.trend_id  = src.trend_id
          WHERE tgt.asset_id IS NULL OR src._valid_from > tgt._valid_to
          ;
          -- there is a flaw somewhere in the above logic.  this update is to correct for that.
          UPDATE transformed.comfort_occupancy AS tgt
          SET 
            tgt._valid_to = '9999-12-31 23:59:59.999'
          FROM new_occupancy_values AS src
          WHERE 
              tgt.trend_id  = src.trend_id 
          AND tgt._valid_from = '0000-01-01 00:00:00.000'
          AND tgt._valid_to = '0000-01-01 00:00:00.001'
          ;

      COMMIT;
      
      END;
  $$
  ;   