-- ------------------------------------------------------------------------------------------------------------------------------
-- create View
-- ------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW transformed.ccure_trend_ids_v AS

SELECT
	c.site_id,
	c.site_name,
	c.time_zone,
	c.trend_id,
	c.id AS capability_id,
	c.model_id,
	c.capability_name,
	c.description AS capability_description,
	c.asset_id,
	c.asset_name,
	split_part(replace(c.model_id_asset, ';',':'),':',4) AS category_name,
	c.asset_detail,
	c.external_id,
	c.unique_id,
	l.level_name,
	l.floor_sort_order,
	space.space_id,
	sl.space_name,
	sl.space_capacity,
	sl.seating_capacity,
	l.custom_properties:capacity.maxOccupancy ::STRING AS max_occupancy
FROM transformed.capabilities_assets c
LEFT JOIN transformed.assets_space space 
		ON (c.asset_id = space.asset_id)
LEFT JOIN transformed.spaces_levels sl
		ON (space.space_id = sl.id)
LEFT JOIN transformed.levels_buildings l 
		ON (sl.level_id = l.id)
WHERE (c.model_id in ('dtmi:com:willowinc:TotalEnteringPeopleCountSensor;1', 'dtmi:com:willowinc:UniqueEnteringPeopleCountSensor;1') 
   OR (c.model_id in ('dtmi:com:willowinc:PeopleCountSensor;1') AND c.capability_name IN ('Total People Count - C-Cure','Unique People Count - C-Cure')))
  AND c.model_id_asset in (
		'dtmi:com:willowinc:PeopleCountSensorEquipment;1',
		'dtmi:com:willowinc:Company;1',
		'dtmi:com:willowinc:Building;1',
		'dtmi:com:willowinc:Door;1'
    )
;
CREATE OR REPLACE TABLE transformed.ccure_trend_ids AS SELECT * FROM transformed.ccure_trend_ids_v;
