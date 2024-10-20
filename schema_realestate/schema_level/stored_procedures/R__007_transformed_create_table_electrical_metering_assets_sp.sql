-- ******************************************************************************************************************************
-- Stored procedure that persists view logic as tables for better performance
-- ******************************************************************************************************************************

CREATE OR REPLACE PROCEDURE transformed.transformed_create_table_electrical_metering_assets_sp()
    RETURNS STRING
    LANGUAGE SQL
	EXECUTE AS CALLER
  AS
    $$

	BEGIN

		CREATE OR REPLACE TABLE transformed.electrical_metering_assets AS
				SELECT * FROM (
					WITH cte_top_level_contains_sensors AS (
					SELECT DISTINCT asset_id
					FROM transformed.capabilities_assets
					WHERE model_id_asset IN ('dtmi:com:willowinc:ElectricalMeter;1','dtmi:com:willowinc:Switchboard;1','dtmi:com:willowinc:Switchgear;1')
					  AND model_id IN ('dtmi:com:willowinc:TotalActiveElectricalPowerSensor;1','dtmi:com:willowinc:ActiveElectricalPowerSensor;1', 'dtmi:com:willowinc:ElectricalPowerSensor;1', 'dtmi:com:willowinc:TotalActiveElectricalEnergySensor;1','dtmi:com:willowinc:ActiveElectricalEnergySensor;1','dtmi:com:willowinc:ElectricalEnergySensor;1')
					)
					SELECT a.* 
					FROM transformed.electrical_metering_assets_v a
						JOIN cte_top_level_contains_sensors cte_top 
						  ON (a.asset_id = cte_top.asset_id)

					UNION

					SELECT a.* 
					FROM transformed.electrical_metering_assets_v a
						LEFT JOIN cte_top_level_contains_sensors cte_top 
							   ON (cte_top.asset_id = a.top_level_asset_id)
					WHERE cte_top.asset_id IS NULL
					AND IFNULL(a.level_2_model_id,'') NOT IN ('dtmi:com:willowinc:ElectricalPanelboard;1')
				);
			
		// create table with all assets for use in hierarchical pivot table
		CALL transformed.transformed_create_table_electrical_metering_hierarchy_sp();
			
	END
		$$
	;

CALL transformed.transformed_create_table_electrical_metering_assets_sp();

CALL transformed.transformed_create_table_electrical_metering_hierarchy_sp();