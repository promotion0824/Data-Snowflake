-- ******************************************************************************************************************************
-- Create view for Comfort dashboard
-- This is used to filter all assets to the models that roll up to AirHandlingUnit
-- ******************************************************************************************************************************
CREATE OR REPLACE VIEW transformed.return_air_handling_unit_assets AS

	WITH cte_return_air_handling_units_and_descendants AS (

		SELECT
		  DISTINCT model_id, extends_model_id,display_name_en
		FROM transformed.ontology_model_hierarchy
		START WITH model_id IN ('dtmi:com:willowinc:AirHandlingUnit;1', 'dtmi:com:willowinc:HVACEquipmentGroup;1')
		CONNECT BY extends_model_id = PRIOR model_id
	)
	SELECT
		   t.model_id,
		   t.twin_id AS asset_id,
		   t.name as asset_name,
		   CASE WHEN ah.model_id like 'dtmi:com:willowinc:HVAC%' THEN 'dtmi:com:willowinc:HVACEquipmentGroup;1' ELSE 'dtmi:com:willowinc:AirHandlingUnit;1' END AS asset_class,
		   display_name_en,
		   floor_id,
		   t.site_id
	FROM cte_return_air_handling_units_and_descendants ah
		JOIN transformed.twins t 
		  ON (t.model_id = ah.model_id)
	WHERE IFNULL(t.is_deleted,false) = false
;