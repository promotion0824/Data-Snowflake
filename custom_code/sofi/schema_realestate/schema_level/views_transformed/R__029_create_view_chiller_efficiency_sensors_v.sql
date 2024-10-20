-- ******************************************************************************************************************************
-- custom report
-- ******************************************************************************************************************************
CREATE OR REPLACE VIEW transformed.chiller_efficiency_sensors_v AS
WITH cte_measurements_sensors AS (
	SELECT
        ca.asset_id,
		ca.model_id_asset,
		ca.asset_name,
		COALESCE(ca.floor_id,t.unique_id,spaces_levels.floor_id) AS floor_id,
		ca.model_id AS model_id_capability,
		o.display_name AS sensor_type,
		ca.capability_name,
		ca.trend_id,
		ca.unit,
        ca.model_id,
        ca.site_id,
        spaces_levels.space_name,
        spaces_levels.model_id AS space_type
		FROM transformed.ontology_buildings o
		JOIN transformed.capabilities_assets ca 
		  ON (o.id = ca.model_id)
        LEFT JOIN transformed.twins_relationships_deduped tr
          ON (ca.asset_id = tr.source_twin_id)
         AND (tr.relationship_name = 'locatedIn')
         AND (tr.is_deleted = FALSE)
        LEFT JOIN transformed.twins t 
          ON (tr.target_twin_id = t.twin_id)
         AND (t.model_id = 'dtmi:com:willowinc:Level;1')
         AND (t.is_deleted = FALSE)
		LEFT JOIN transformed.assets_space assets_space
		  ON (ca.asset_id = assets_space.asset_id)
		LEFT JOIN transformed.spaces_levels spaces_levels
		  ON (assets_space.space_id = spaces_levels.id)
		WHERE 
				o.model_level_2 = 'Performance Indicator'
			AND o.model_level_3 = 'Ratio'
)
,cte_run_sensors AS (
	SELECT
		ca.asset_id,
		ca.model_id_asset,
		ca.asset_name,
		ca.floor_id,
		ca.model_id AS model_id_capability,
		ca.capability_name,
		ca.trend_id,
		ca.unit,
		ca.model_id,
		ca.site_id
		FROM transformed.capabilities_assets ca
		WHERE
            ca.model_id IN ('dtmi:com:willowinc:CompressorRunSensor;1')
)
    SELECT 
        measurements.asset_id,
		measurements.model_id_asset,
		measurements.asset_name,
		measurements.model_id_capability,
		measurements.sensor_type,
		measurements.capability_name,
		measurements.trend_id AS sensor_trend_id,
        run.trend_id AS run_sensor_trend_id,
		measurements.unit,
		measurements.floor_id,
		run.model_id_capability AS run_sensor_model_id,
        measurements.site_id,
        measurements.space_name,
        measurements.space_type
FROM cte_measurements_sensors measurements
    LEFT JOIN cte_run_sensors run
           ON (measurements.asset_id = run.asset_id)
;

CREATE OR REPLACE TRANSIENT TABLE transformed.chiller_efficiency_sensors AS SELECT * FROM transformed.chiller_efficiency_sensors_v;