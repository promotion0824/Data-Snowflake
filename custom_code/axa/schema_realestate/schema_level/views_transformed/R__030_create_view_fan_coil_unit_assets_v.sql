-- ******************************************************************************************************************************
-- Create view 
-- ******************************************************************************************************************************
CREATE OR REPLACE VIEW transformed.fan_coil_unit_assets_v AS

	SELECT
        ca.asset_id,
        ca.model_id_asset,
        ca.asset_name,
        COALESCE(ca.floor_id,t.unique_id,spaces_levels.floor_id) AS floor_id,
        ca.model_id AS model_id_capability,
        ca.capability_name,
        ca.trend_id,
        ca.unit,
        ca.model_id,
        ca.site_id,
        ca.site_name,
        spaces_levels.space_name,
        spaces_levels.model_id AS space_type,
        l.level_name,
        l.floor_sort_order
		FROM transformed.capabilities_assets ca
        LEFT JOIN transformed.sites s 
          ON (ca.site_id = s.site_id)
        LEFT JOIN transformed.twins_relationships_deduped tr
          ON (ca.asset_id = tr.source_twin_id)
         AND tr.relationship_name = 'locatedIn'
         AND tr.is_deleted = FALSE
        LEFT JOIN transformed.twins t 
          ON (tr.target_twin_id = t.twin_id)
         AND (t.model_id = 'dtmi:com:willowinc:Level;1')
         AND t.is_deleted = FALSE
		LEFT JOIN transformed.assets_space assets_space
		  ON (ca.asset_id = assets_space.asset_id)
		LEFT JOIN transformed.spaces_levels spaces_levels
		  ON (assets_space.space_id = spaces_levels.id)
        LEFT JOIN transformed.levels_buildings l 
          ON (spaces_levels.floor_id = l.unique_id)
		WHERE 
                ca.model_id_asset IN ('dtmi:com:willowinc:FanCoilUnit;1')
            AND ca.model_id IN ('dtmi:com:willowinc:ZoneAirTemperatureSensor;1','dtmi:com:willowinc:ModeSensor;1','dtmi:com:willowinc:ModeState;1')
            AND IFNULL(space_type,'') != 'dtmi:com:willowinc:Level;1'
;

CREATE OR REPLACE TRANSIENT TABLE transformed.fan_coil_unit_assets AS SELECT * FROM transformed.fan_coil_unit_assets_v;