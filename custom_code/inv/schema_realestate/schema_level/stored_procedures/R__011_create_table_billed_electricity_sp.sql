-- ******************************************************************************************************************************
-- Stored procedure that incrementally populates the air_temperature_measurements table
-- ******************************************************************************************************************************

CREATE OR REPLACE PROCEDURE transformed.create_table_billed_electricity_sp()
    RETURNS STRING
    LANGUAGE SQL
  AS
    $$
    CREATE OR REPLACE TABLE transformed.billed_electricity AS
    SELECT 
        date_local,
        timestamp_local,
        telemetry_value,
        capability_id,
        ca.trend_id,
        model_id,
        unit,
        asset_id,
        model_id_asset,
        ca.site_id,
        ca.building_id,
        ca.building_name,
        SYSDATE() AS _last_updated_at
    FROM transformed.telemetry ts
    JOIN transformed.capabilities_assets ca ON ca.capability_id = ts.dt_id
    WHERE ca.model_id IN ('dtmi:com:willowinc:BilledActiveElectricalEnergy;1','dtmi:com:willowinc:BilledElectricalCost;1')
    AND ts.date_local >= '2022-04-30';
    $$
;