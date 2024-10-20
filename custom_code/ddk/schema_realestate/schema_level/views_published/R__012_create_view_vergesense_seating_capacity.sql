-- ------------------------------------------------------------------------------------------------------------------------------
-- create View for reporting
-- ------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE VIEW published.vergesense_seating_capacity AS

SELECT 
	site_id,
	building_id,
	building_name,
	asset_id, 
	asset_name,
	MAX(seating_capacity) AS seating_capacity
FROM transformed.vergesense_assets 
WHERE seating_capacity IS NOT NULL
GROUP BY 
	site_id,
	building_id,
	building_name,
	asset_id,
	asset_name;