-- ******************************************************************************************************************************
-- Create view
-- ******************************************************************************************************************************
CREATE OR REPLACE VIEW transformed.capabilities_assets_v AS

	WITH cte_land AS (
		SELECT 
			land_id,
			SUM(gross_area) AS gross_area,
			ANY_VALUE(gross_area_unit) AS gross_area_unit,
			ANY_VALUE(time_zone) AS time_zone
		FROM transformed.building_land_rollup
		GROUP BY land_id
	)
		SELECT
		r.source_twin_id AS id,
		ts.twin_id AS capability_id,
		ts.name AS capability_name,
		LOWER(ts.trend_id) AS trend_id,
		ts.raw_json_value:customProperties.trendInterval::INTEGER AS trend_interval,
		ts.model_id,
		ts.unique_id,
		ts.external_id,
		ts.raw_json_value:customProperties.unit::VARCHAR(100) AS unit,
		ts.raw_json_value:customProperties.tags::VARIANT AS tags,
		ts.tags AS tags_string,
		ts.raw_json_value:customProperties.enabled::INTEGER AS enabled,
		ts.raw_json_value:customProperties.type::VARCHAR(100) AS capability_type,
		ts.raw_json_value:customProperties.description::VARCHAR(1000) AS description,
        tt.raw_json_value:customProperties.comments::VARCHAR(2000) AS comments,
		COALESCE(tt.site_dtid,ts.site_dtid, t2.site_dtid,
            CASE WHEN t2.model_id IN ('dtmi:com:willowinc:Land;1','dtmi:com:willowinc:Building;1') THEN t2.twin_id ELSE NULL END,
            t3.site_dtid,
            CASE WHEN t3.model_id IN ('dtmi:com:willowinc:Land;1','dtmi:com:willowinc:Building;1') THEN t3.twin_id ELSE NULL END
        ) AS site_all_id,
		site_all_id AS building_id,
		b.building_name,
		COALESCE(tt.site_id,ts.site_id) AS site_id,
		COALESCE(b.time_zone, l.time_zone) AS time_zone,
		COALESCE(b.gross_area, l.gross_area) AS gross_area,
		COALESCE(b.gross_area_unit, l.gross_area_unit) AS gross_area_unit,
		b.site_name,
        -- get site_name from dtid???????; need to POPULATE building_id, building_name, site_name WITH THE land twins;
		r.target_twin_id AS asset_id,
		tt.name AS asset_name,
		tt.raw_json_value AS asset_detail,
		tt.model_id AS model_id_asset,
		SPLIT_PART(REPLACE(tt.model_id, ';',':'),':',4) AS model_name_asset,
		COALESCE(tt.floor_id,ts.floor_id) AS floor_id,
		COALESCE(tt.floor_dtid, ts.floor_dtid) AS floor_dtid,
		ts.raw_json_value:customProperties::VARIANT AS custom_properties,
		model_level_1,
		model_level_2,
		model_level_3,
		model_level_4,
		model_level_5,
		ts.is_deleted source_twin_is_deleted,
        tt.is_deleted AS target_twin_is_deleted,
        r.is_deleted AS relationship_is_deleted,
		ts._stage_record_id,
		ts._loader_run_id,
		ts._ingested_at,
		ts._staged_at
	FROM transformed.twins ts
		LEFT JOIN transformed.ontology_buildings os ON (ts.model_id = os.id)
		JOIN transformed.twins_relationships_deduped r ON (ts.twin_id = r.source_twin_id)
		JOIN transformed.twins tt ON (tt.twin_id = r.target_twin_id)
        -- get building when site_dtid is not populated
        LEFT JOIN transformed.twins_relationships_deduped r2 ON asset_id = r2.source_twin_id AND r2.relationship_name in ('locatedIn','serves')
        LEFT JOIN transformed.twins t2 ON r2.target_twin_id = t2.twin_id
        -- get the level for the room;
        LEFT JOIN transformed.twins_relationships_deduped r3 ON t2.twin_id = r3.source_twin_id AND r3.relationship_name in ('isPartOf','locatedIn')
        LEFT JOIN transformed.twins t3 ON r3.target_twin_id = t3.twin_id

		LEFT JOIN transformed.buildings b
		  ON (site_all_id = b.building_id)
		LEFT JOIN cte_land l
		  ON (site_all_id = l.land_id)
	WHERE r.relationship_name IN ('isCapabilityOf') 
		AND IFNULL(r.is_deleted,FALSE) = FALSE
		AND IFNULL(ts.is_deleted,FALSE) = FALSE
		AND IFNULL(tt.is_deleted,FALSE) = FALSE
		AND IFNULL(r2.is_deleted,FALSE) = FALSE
		AND IFNULL(t2.is_deleted,FALSE) = FALSE
QUALIFY ROW_NUMBER() OVER (PARTITION BY capability_id,asset_id ORDER BY r2.relationship_name DESC,r3.relationship_name DESC) = 1;

CREATE OR REPLACE TRANSIENT TABLE transformed.capabilities_assets AS SELECT * FROM transformed.capabilities_assets_v;
