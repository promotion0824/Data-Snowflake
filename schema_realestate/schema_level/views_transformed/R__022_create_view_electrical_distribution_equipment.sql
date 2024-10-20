-- ******************************************************************************************************************************
-- Create view
-- This is used to filter all assets to the models that roll up to ElectricalDistributionEquipment
-- ******************************************************************************************************************************
CREATE OR REPLACE VIEW transformed.electrical_distribution_equipment AS
--  find the lowest level assets that roll up to ElectricalDistributionEquipment
	WITH cte_distribution_equipment AS (
	   SELECT DISTINCT
		   position('ElectricalDistributionEquipment',path) AS begin_position,
		   position('>',path,begin_position+31) AS end_position, 
		   substring(path,begin_position+32,end_position) AS electrical_distribution_equipment_models,
		   path
		FROM transformed.ontology_model_hierarchy o
		WHERE path like '%ElectricalDistributionEquipment%'
		)
	SELECT
		   t.model_id,
		   t.twin_id AS asset_id,
		   t.name as asset_name,
		   'dtmi:com:willowinc:ElectricalDistributionEquipment;1' AS asset_class,
		   t.is_deleted,
		   t.floor_id,
		   t.site_id,
		   path
	FROM cte_distribution_equipment
		JOIN transformed.twins t 
		  ON (electrical_distribution_equipment_models = REPLACE(REPLACE(t.model_id,'dtmi:com:willowinc:',''),';1','') )
	WHERE t.model_id not in ('dtmi:com:willowinc:ElectricalReceptacle;1','')
      AND IFNULL(t.is_deleted,FALSE) = FALSE
	QUALIFY ROW_NUMBER() OVER (PARTITION BY t.twin_id ORDER BY t.export_time desc) = 1

UNION ALL
	SELECT 
		   t.model_id,
		   t.asset_id,
		   t.asset_name,
		   t.model_id_asset AS asset_class,
		   NULL AS is_deleted,
		   t.floor_id,
		   t.site_id,
		   NULL AS path
	FROM transformed.capabilities_assets t
	WHERE t.model_id_asset = 'dtmi:com:willowinc:ElectricalMeter;1'
;