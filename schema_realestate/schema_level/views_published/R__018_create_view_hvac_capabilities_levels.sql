-- ******************************************************************************************************************************
-- Create view
-- ******************************************************************************************************************************

CREATE OR REPLACE VIEW published.hvac_capabilities_levels AS
	SELECT DISTINCT 
		c.capability_name,
		c.capability_id,
		c.ontology_model_level_4,
		c.level_name,
		c.asset_name,
		c.site_id
	FROM transformed.hvac_adjusted_capabilities c
	WHERE c.capability_name NOT ILIKE '%Phase%'
;
