-- ******************************************************************************************************************************
-- Create view
-- This is used to filter all assets to the models that roll up to ElectricalDistributionEquipment
-- ******************************************************************************************************************************
CREATE OR REPLACE VIEW transformed.electrical_distribution_equipment_levels AS
	SELECT DISTINCT 
        a.asset_id,
		de.asset_class,
		a.model_id_asset AS level_3_model_id,
		a.asset_id AS level_3_asset_id,
		a.asset_name AS level_3_asset_name,
		tr.relationship_name AS level_2_asset_relationship_type,
        t.model_id AS level_2_model_id,
		t.asset_id AS level_2_asset_id,
		t.asset_name AS level_2_asset_name,
		REPLACE(tr2.relationship_name,'locatedIn','') AS level_1_asset_relationship_type,
		t2.model_id AS level_1_model_id,
		t2.asset_id AS level_1_asset_id,
		t2.asset_name AS level_1_asset_name,
        COALESCE(level_1_model_id,level_2_model_id,level_3_model_id) AS top_level_model_id,
        COALESCE(level_1_asset_id,level_2_asset_id,level_3_asset_id) AS top_level_asset_id,
        COALESCE(level_1_asset_name,level_2_asset_name,level_3_asset_name) AS top_level_asset_name,
		a.site_id
		FROM transformed.capabilities_assets a
		JOIN transformed.electrical_distribution_equipment de 
		  ON (a.asset_id = de.asset_id)
		LEFT JOIN transformed.twins_relationships_deduped tr 
		  ON (a.asset_id = tr.source_twin_id)
		LEFT JOIN transformed.electrical_distribution_equipment t 
		  ON (tr.target_twin_id = t.asset_id)
		LEFT JOIN transformed.twins_relationships_deduped tr2 
		  ON (t.asset_id = tr2.source_twin_id)
		LEFT JOIN transformed.electrical_distribution_equipment t2
		  ON (tr2.target_twin_id = t2.asset_id)
		WHERE
		 	(IFNULL(tr.relationship_name,'isFedBy') = 'isFedBy' OR tr.relationship_name = 'locatedIn')
        AND IFNULL(tr2.relationship_name,'isFedBy') IN ('isFedBy','locatedIn')
        AND (level_1_model_id IN ('dtmi:com:willowinc:ElectricalPanelboard;1','dtmi:com:willowinc:Switchboard;1','dtmi:com:willowinc:Switchgear;1','dtmi:com:willowinc:ElectricalPanelboardMLO;1','dtmi:com:willowinc:ElectricalPanelboardMCB;1') OR level_1_model_id IS NULL OR model_id_asset='dtmi:com:willowinc:ElectricalMeter;1')
        AND (level_2_model_id IN ('dtmi:com:willowinc:ElectricalPanelboard;1','dtmi:com:willowinc:Switchboard;1','dtmi:com:willowinc:Switchgear;1','dtmi:com:willowinc:ElectricalPanelboardMLO;1','dtmi:com:willowinc:ElectricalPanelboardMCB;1') OR level_2_model_id IS NULL OR model_id_asset='dtmi:com:willowinc:ElectricalMeter;1')
		AND IFNULL(t.is_deleted,FALSE) = FALSE
        AND IFNULL(tr.is_deleted,FALSE) = FALSE
        AND IFNULL(tr2.is_deleted,FALSE) = FALSE
		AND IFNULL(t2.is_deleted,FALSE) = FALSE
	QUALIFY ROW_NUMBER() OVER (PARTITION BY a.asset_id ORDER BY a._ingested_at DESC) = 1
;