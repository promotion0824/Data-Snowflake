-- ******************************************************************************************************************************
-- custom report
-- ******************************************************************************************************************************
CREATE OR REPLACE VIEW transformed.chiller_assets_v AS
WITH cte_delta_sensors AS (
	SELECT
        ca.asset_id,
		ca.model_id_asset,
		ca.asset_name,
		COALESCE(ca.floor_id,t.unique_id,spaces_levels.floor_id) AS floor_id,
		ca.model_id AS model_id_capability,
		ca.trend_id,
		ca.unit,
        ca.model_id,
        ca.site_id,
        spaces_levels.space_name,
        spaces_levels.model_id AS space_type
		FROM transformed.capabilities_assets ca
        LEFT JOIN transformed.twins_relationships_deduped tr
          ON (ca.asset_id = tr.source_twin_id)
         AND tr.relationship_name = 'locatedIn'
         AND tr.is_deleted = FALSE
        LEFT JOIN transformed.twins t 
          ON (tr.target_twin_id = t.twin_id)
         AND (t.model_id = 'dtmi:com:willowinc:Level;1')
         AND t.is_deleted = FALSE
		LEFT JOIN transformed.assets_space assets_space
		  ON (ca.asset_id = assets_space.asset_id)
		LEFT JOIN transformed.spaces_levels spaces_levels
		  ON (assets_space.space_id = spaces_levels.id)
		WHERE (ca.model_level_4 = 'Chiller' 
		   OR  ca.model_id_asset = 'dtmi:com:willowinc:WaterCooledChiller;1')
        AND ca.model_id IN ('dtmi:com:willowinc:DeltaChilledWaterTemperatureSensor;1',	'dtmi:com:willowinc:ChilledWaterDeltaPressureSensor;1')
)
,cte_run_sensors AS (
	SELECT
		ca.asset_id,
		ca.model_id_asset,
		ca.asset_name,
		a.floor_id,
		ca.model_id AS model_id_capability,	
		ca.trend_id,
		ca.unit,
		ca.model_id,
		ca.site_id
		FROM transformed.capabilities_assets ca
		JOIN transformed.terminal_units_assets a 
		  ON (ca.asset_id = a.asset_id)
		WHERE
            ca.model_id IN ('dtmi:com:willowinc:CompressorRunSensor;1')
)
    SELECT 
    
        delta.asset_id,
		delta.model_id_asset,
		delta.asset_name,
		delta.model_id_capability,
		delta.trend_id AS sensor_trend_id,
        run.trend_id AS run_sensor_trend_id,
		delta.unit,
		delta.floor_id,
		run.model_id_capability AS run_sensor_model_id,
        delta.site_id,
        delta.space_name,
        delta.space_type
FROM cte_delta_sensors delta
    LEFT JOIN cte_run_sensors run
           ON (delta.asset_id = run.asset_id)
          AND (delta.unit = run.unit)
;

CREATE OR REPLACE TRANSIENT TABLE transformed.chiller_assets AS SELECT * FROM transformed.chiller_assets_v;