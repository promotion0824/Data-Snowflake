-- ******************************************************************************************************************************
-- Create view
-- This is used to filter all assets to the models that roll up to MeterEquipment
-- ******************************************************************************************************************************
CREATE OR REPLACE VIEW transformed.meter_equipment AS
--  find the lowest level assets that roll up to MeterEquipment
	WITH cte_distribution_equipment AS (
	   SELECT DISTINCT
		   position('MeterEquipment',path) AS begin_position,
		   position('>',path,begin_position+14) AS end_position, 
		   substring(path,begin_position+15,end_position) AS meter_equipment_models,
		   path
		FROM transformed.ontology_model_hierarchy o
		WHERE path like '%MeterEquipment%'
		)
	SELECT
		   t.model_id,
		   t.twin_id AS asset_id,
		   t.name as asset_name,
		   'dtmi:com:willowinc:MeterEquipment;1' AS asset_class,
		   t.is_deleted,
		   t.floor_id,
		   t.site_id,
		   path
	FROM cte_distribution_equipment
		JOIN transformed.twins t 
		  ON (meter_equipment_models = REPLACE(REPLACE(t.model_id,'dtmi:com:willowinc:',''),';1','') )
	WHERE IFNULL(t.is_deleted,FALSE) = FALSE
	QUALIFY ROW_NUMBER() OVER (PARTITION BY t.twin_id ORDER BY t.export_time desc) = 1
;