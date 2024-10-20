-- ******************************************************************************************************************************
-- Create view
-- ******************************************************************************************************************************

CREATE OR REPLACE VIEW transformed.occupancy_v AS
SELECT
		ca.capability_name,
		ca.trend_id,
		ca.model_id AS model_id_capability,
		ca.capability_type,
		ca.building_id,
		ca.building_name,
		ca.site_id,
		ca.site_name,
		ca.time_zone,
		ca.id AS sensor_id,
		sp.space_id,
		sp.space_name,
		SPLIT_PART(REPLACE(l.model_id, ';',':'),':',4) AS space_type,
		l.space_capacity AS capacity,
		l.usable_area_space AS usable_area,
		l.level_name,
		l.level_id,
		l.floor_sort_order
FROM transformed.capabilities_assets ca
LEFT JOIN transformed.assets_space sp 
  	   ON (ca.asset_id = sp.asset_id)
LEFT JOIN transformed.spaces_levels l 
  	  ON (sp.space_id = l.id)
WHERE 
        ca.model_id ILIKE ANY ('dtmi:com:willowinc:OccupancySensor;1','dtmi:com:willowinc:OccupiedState;1','dtmi:com:willowinc:OccupiedUnoccupiedState;1')
    AND ca.model_id_asset IN ('dtmi:com:willowinc:OccupancyZone;1','dtmi:com:willowinc:Room;1','dtmi:com:willowinc:Level;1')
;

CREATE OR REPLACE TABLE transformed.occupancy AS SELECT * FROM transformed.occupancy_v;
