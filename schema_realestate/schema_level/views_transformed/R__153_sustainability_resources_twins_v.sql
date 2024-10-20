-- ******************************************************************************************************************************
-- Create view and persist as a table in transformed
-- ******************************************************************************************************************************
CREATE OR REPLACE VIEW transformed.sustainability_resources_twins_v AS
SELECT DISTINCT
        m.resource_type,
        m.data_type,
        ca.capability_id,
        ca.capability_name,
        ca.trend_id,
        ca.model_id,
        NULLIF(ca.external_id,'') AS external_id,
        ca.unit,
        ca.asset_id,
        ca.asset_name,
        ca.model_id_asset,
        tr.relationship_name,
        tt.name AS provider_name,
        tt.twin_id AS provider_id,
        COALESCE(NULLIF(ca.asset_detail:customProperties.emissionFactor.co2e::STRING,''), NULLIF(ca.asset_detail:customProperties.emissionFactor.co2::STRING,''),NULLIF(tt.raw_json_value:customProperties.emissionFactor.co2e::STRING,''), NULLIF(tt.raw_json_value:customProperties.emissionFactor.co2::STRING,'')) AS emissions_factor,        
        COALESCE(NULLIF(ca.asset_detail:customProperties.emissionFactor.co2eUnit::STRING,''), NULLIF(ca.asset_detail:customProperties.emissionFactor.co2Unit::STRING,''),NULLIF(tt.raw_json_value:customProperties.emissionFactor.co2eUnit::STRING,''), NULLIF(tt.raw_json_value:customProperties.emissionFactor.co2Unit::STRING,'')) AS emissions_factor_unit,
        m.emissions_factor_source,
        CASE WHEN resource_type IN ('Electricity','Steam') THEN 'Scope 2' 
                WHEN resource_type IN ('Natural Gas') THEN 'Scope 1'
                ELSE 'N/A' 
        END AS emissions_scope,
        ca.site_id,
        ca.site_name,	
        b.time_zone,
        b.customer_id,
        b.portfolio_id,
        ca.building_id,
        ca.building_name,
        b.type AS building_type,
        b.custom_properties:type::STRING AS property_type,
        LEFT(b.custom_properties:constructionCompletionDate::STRING,4) AS property_year_built,
        COALESCE(b.gross_area,b.rentable_area) AS building_gross_area,
        IFNULL(b.gross_area_unit,'sf') AS building_gross_area_unit,
        COALESCE(b.rentable_area,b.gross_area) AS building_rentable_area,
        b.custom_properties:address.region::STRING AS building_region,
        b.custom_properties:address.city::STRING AS city
   FROM transformed.sustainability_resources_models m
        JOIN transformed.capabilities_assets ca 
          ON m.capablity_model_id = ca.model_id
         --AND ca.model_id_asset IN ('dtmi:com:willowinc:UtilityAccount;1', 'dtmi:com:willowinc:Building;1', 'dtmi:com:willowinc:BuildingTower;1', 'dtmi:com:willowinc:Substructure;1')
   LEFT JOIN transformed.twins_relationships_deduped tr
          ON ca.asset_id = tr.source_twin_id
         AND tr.relationship_name IN ('isProvidedBy','isLinkedTo')
   LEFT JOIN transformed.twins tt
          ON tr.target_twin_id = tt.twin_id
         AND tt.model_id IN ('dtmi:com:willowinc:Company;1','dtmi:com:willowinc:UtilityAccount;1')
   LEFT JOIN transformed.twins_relationships_deduped tr2
          ON ca.asset_id = tr2.source_twin_id
         AND tr2.relationship_name = 'serves'
   LEFT JOIN transformed.twins tt2
          ON tr2.target_twin_id = tt2.twin_id
   LEFT JOIN transformed.levels_buildings l
          ON (ca.floor_id = l.floor_id)
   LEFT JOIN transformed.buildings b 
          ON (ca.site_id = b.site_id)
    WHERE 
	   IFNULL(ca.enabled,true) = true	
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ca.id,ca.asset_id ORDER BY ca.source_twin_is_deleted, ca.target_twin_is_deleted, ca._staged_at DESC) = 1
;


CREATE OR REPLACE TABLE transformed.sustainability_resources_twins AS SELECT * FROM transformed.sustainability_resources_twins_v;