-- ******************************************************************************************************************************
-- Stored procedure that incrementally populates the air_temperature_measurements table
-- ******************************************************************************************************************************

CREATE OR REPLACE PROCEDURE transformed.create_table_billed_electricity_sp(task_name VARCHAR)
    RETURNS STRING
    LANGUAGE SQL
  AS
    $$
    CREATE OR REPLACE TABLE transformed.billed_electricity AS
    SELECT 
        date_local,
        timestamp_local,
        telemetry_value,
        capability_name,
        ca.trend_id,
        model_id,
        unit,
        description,
        asset_name,
        model_id_asset,
        ca.site_id
    FROM transformed.capabilities_assets ca
    JOIN transformed.telemetry ts on ca.trend_id = ts.trend_id
    WHERE model_id IN ('dtmi:com:willowinc:BilledActiveElectricalEnergy;1','dtmi:com:willowinc:BilledElectricalCost;1');
    $$
;