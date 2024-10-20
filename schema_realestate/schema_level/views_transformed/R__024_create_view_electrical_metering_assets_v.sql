-- ******************************************************************************************************************************
-- Create view and persist as a table in transformed
-- ******************************************************************************************************************************
CREATE OR REPLACE VIEW transformed.electrical_metering_assets_v AS
SELECT DISTINCT
		del.asset_class,
		ca.capability_id,
		ca.capability_name,
		ca.trend_id,
		ca.trend_interval,
		ca.model_id,
		ca.unique_id,
		ca.external_id,
		ca.unit,
		ca.tags,
		ca.tags_string,
		ca.enabled,
		ca.capability_type,
		ca.description,
		ca.asset_id,
		ca.asset_name,
		ca.comments AS asset_display_name,
		del.level_3_model_id,
		del.level_3_asset_id,
		del.level_3_asset_name,
		del.level_2_asset_relationship_type,
        del.level_2_model_id,
		del.level_2_asset_id,
		del.level_2_asset_name,		
		del.level_1_asset_relationship_type,
		del.level_1_model_id,
		del.level_1_asset_id,
		del.level_1_asset_name,
        COALESCE(del.top_level_model_id,ca.model_id_asset) AS top_level_model_id,
        COALESCE(del.top_level_asset_id,ca.asset_id) AS top_level_asset_id,
        COALESCE(del.top_level_asset_name,ca.asset_name) AS top_level_asset_name,
		ca.model_id_asset,
		ca.floor_id,
		l.level_name, 
		l.floor_sort_order,
		ca.site_id,
		s.name AS site_name,
		s.time_zone,
		s.customer_id,
		s.portfolio_id,
		ca.building_id,
		ca.building_name,
		b.type AS building_type,
		COALESCE(b.gross_area,b.rentable_area) AS building_gross_area,
		b.gross_area_unit AS building_gross_area_unit,
		COALESCE(b.rentable_area,b.gross_area) AS building_rentable_area,
		mc.model_id AS capability_model_class,
		CASE WHEN ca.model_id ILIKE '%Power%'  THEN 'Power' 
			 WHEN ca.model_id ILIKE '%Energy%' THEN 'Energy' 
			 ELSE null 
	    END AS sensor_type,
		ca.source_twin_is_deleted,
        ca.target_twin_is_deleted,
        ca.relationship_is_deleted
	FROM transformed.capabilities_assets ca
        JOIN transformed.electrical_distribution_equipment de 
		  ON (ca.asset_id = de.asset_id)
		JOIN transformed.ontology_model_hierarchy mc 
		  ON (ca.model_id = mc.model_id)
		JOIN transformed.ontology_model_hierarchy ma 
		  ON (ca.model_id_asset = ma.model_id)
		LEFT JOIN transformed.levels_buildings l
          ON (ca.floor_id = l.floor_id)
		LEFT JOIN transformed.buildings b 
		  ON (ca.site_id = b.site_id)
		LEFT JOIN transformed.sites s 
		  ON (ca.site_id = s.site_id)
		LEFT JOIN transformed.electrical_distribution_equipment_levels del
		  ON (ca.asset_id = del.level_3_asset_id)
    WHERE 
		 capability_model_class IN ('dtmi:com:willowinc:ActiveElectricalPowerSensor;1',
									'dtmi:com:willowinc:ActiveElectricalEnergySensor;1',
									'dtmi:com:willowinc:ElectricalPowerSensor;1',
									'dtmi:com:willowinc:ElectricalEnergySensor;1',
									'dtmi:com:willowinc:TotalActiveElectricalPowerSensor;1',
									'dtmi:com:willowinc:TotalActiveElectricalEnergySensor;1'
									)
	AND IFNULL(ca.enabled,true) = true	
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ca.id,ca.asset_id ORDER BY ca.source_twin_is_deleted, ca.target_twin_is_deleted, ca._staged_at DESC) = 1
;

CREATE OR REPLACE TABLE transformed.electrical_metering_assets AS SELECT * FROM transformed.electrical_metering_assets_v;