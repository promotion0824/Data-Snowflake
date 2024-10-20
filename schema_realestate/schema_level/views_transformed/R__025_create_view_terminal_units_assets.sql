-- ******************************************************************************************************************************
-- Create view for Comfort dashboard
-- This is used to filter all assets to the models that roll up to TerminalUnit
-- ******************************************************************************************************************************
CREATE OR REPLACE VIEW transformed.terminal_units_assets AS
    WITH cte_terminal_units_and_descendants AS (

		SELECT
		  DISTINCT 
          model_id, 
          extends_model_id,
          display_name_en,
          CASE WHEN model_id IN ('dtmi:com:willowinc:TerminalUnit;1', 'dtmi:com:willowinc:AirHandlingUnit;1') THEN model_id ELSE NULL END AS model_class,
          CASE WHEN extends_model_id IN ('dtmi:com:willowinc:TerminalUnit;1', 'dtmi:com:willowinc:AirHandlingUnit;1') THEN extends_model_id ELSE NULL END AS extends_class
		FROM transformed.ontology_model_hierarchy
		START WITH model_id IN ('dtmi:com:willowinc:TerminalUnit;1', 'dtmi:com:willowinc:AirHandlingUnit;1')
		CONNECT BY extends_model_id = PRIOR model_id
	)
	SELECT
		   t.model_id, 
		   t.twin_id AS asset_id,
		   t.name as asset_name,
           COALESCE(extends_class,model_class) AS asset_class,
		   display_name_en,
		   floor_id,
		   t.site_id
	FROM cte_terminal_units_and_descendants tu
		JOIN transformed.twins t 
		  ON (t.model_id = tu.model_id)
	WHERE IFNULL(t.is_deleted,false) = false
;