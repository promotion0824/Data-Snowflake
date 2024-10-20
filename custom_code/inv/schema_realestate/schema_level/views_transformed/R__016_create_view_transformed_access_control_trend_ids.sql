-- ------------------------------------------------------------------------------------------------------------------------------
-- create View
-- ------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW transformed.access_control_trend_ids_v AS
SELECT
 c.trend_id,
 c.id AS capability_id,
 c.model_id,
 c.capability_name,
 c.description AS capability_description,
 c.asset_id,
 c.asset_name,
 split_part(replace(c.model_id_asset, ';',':'),':',4) AS category_name,
 l.level_name,
 l.level_number,
 l.level_code,
 l.floor_sort_order,
 c.building_id,
 c.building_name,
 c.site_id,
 s.name AS site_name,
 s.time_zone
FROM transformed.capabilities_assets c
LEFT JOIN transformed.sites s
       ON (c.site_id = s.site_id)
LEFT JOIN transformed.levels_buildings l on c.asset_id = l.id
where c.model_id in (
'dtmi:com:willowinc:PeopleCountSensor;1',
'dtmi:com:willowinc:TotalEnteringPeopleCountSensor;1',
'dtmi:com:willowinc:UniqueEnteringPeopleCountSensor;1',
'dtmi:com:willowinc:TotalLeavingPeopleCountSensor;1',
'dtmi:com:willowinc:UniqueLeavingPeopleCountSensor;1')
;
CREATE OR REPLACE TABLE transformed.access_control_trend_ids AS SELECT * FROM transformed.access_control_trend_ids_v;
