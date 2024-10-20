-- ******************************************************************************************************************************
-- Create view
-- ******************************************************************************************************************************

CREATE OR REPLACE VIEW published.hvac_equipment_levels AS
	SELECT DISTINCT
		c.asset_name,
		c.adjusted_capability_name,
		c.space_name,
		c.space_id,
		c.level_name,
		c.level_id,
		c.site_id
	FROM transformed.hvac_adjusted_capabilities c
	WHERE c.adjusted_capability_name IS NOT NULL
;