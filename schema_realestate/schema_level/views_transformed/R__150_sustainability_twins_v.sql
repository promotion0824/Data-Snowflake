-- ******************************************************************************************************************************
-- Create view and persist as a table in transformed
-- ******************************************************************************************************************************
CREATE OR REPLACE VIEW transformed.sustainability_twins_v AS
SELECT DISTINCT
		ca.capability_id,
		ca.capability_name,
		ca.trend_id,
		ca.model_id,
		NULLIF(ca.external_id,'') AS external_id,
		ca.unit,
		ca.asset_id,
		ca.asset_name,
        ca.model_id_asset,
		CASE 
			WHEN ca.model_id IN ('dtmi:com:willowinc:BilledElectricalCost;1','dtmi:com:willowinc:BilledActiveElectricalEnergy;1')
				THEN 'Electricity'
			WHEN ca.model_id IN ('dtmi:com:willowinc:BilledNaturalGasEnergy;1', 'dtmi:com:willowinc:BilledNaturalGasCost;1')
				THEN 'Natural Gas'
			WHEN ca.model_id IN ('dtmi:com:willowinc:BilledWaterVolume;1', 'dtmi:com:willowinc:BilledWaterCost;1')
				THEN 'Domestic Water'
			WHEN ca.model_id IN ('dtmi:com:willowinc:BilledReclaimedWaterVolume;1', 'dtmi:com:willowinc:BilledReclaimedWaterCost;1')
				THEN 'Reclaim Water'
			ELSE 'Unknown'
		END AS service_type,
        tr.relationship_name,
		COALESCE(tt.name,ca.asset_name) AS provider_name,
        COALESCE(tt.twin_id,ca.asset_id) AS provider_id,
        COALESCE(NULLIF(ca.asset_detail:customProperties.emissionFactor.co2e::STRING,''), NULLIF(ca.asset_detail:customProperties.emissionFactor.co2::STRING,''),NULLIF(tt.raw_json_value:customProperties.emissionFactor.co2e::STRING,'')) AS emissions_factor,
        COALESCE(NULLIF(ca.asset_detail:customProperties.emissionFactor.co2eUnit::STRING,''), NULLIF(ca.asset_detail:customProperties.emissionFactor.co2Unit::STRING,''),NULLIF(tt.raw_json_value:customProperties.emissionFactor.co2eUnit::STRING,'')) AS emissions_factor_unit,
        ca.site_id,
        ca.site_name,	
		s.time_zone,
		s.customer_id,
		s.portfolio_id,
		ca.building_id,
		ca.building_name,
		s.type AS building_type,
		COALESCE(b.gross_area,b.rentable_area) AS building_gross_area,
		b.gross_area_unit AS building_gross_area_unit,
		COALESCE(b.rentable_area,b.gross_area) AS building_rentable_area
   FROM transformed.capabilities_assets ca
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
   LEFT JOIN transformed.sites s 
		  ON (s.site_id = ca.site_id)
    WHERE 
		 ca.model_id IN (
                    'dtmi:com:willowinc:BilledElectricalCost;1',
                    'dtmi:com:willowinc:BilledActiveElectricalEnergy;1',
					'dtmi:com:willowinc:BilledNaturalGasEnergy;1',
					'dtmi:com:willowinc:BilledNaturalGasCost;1',
					'dtmi:com:willowinc:BilledWaterVolume;1',
					'dtmi:com:willowinc:BilledWaterCost;1',
					'dtmi:com:willowinc:BilledReclaimedWaterVolume;1',
					'dtmi:com:willowinc:BilledReclaimedWaterCost;1'
					)
	AND IFNULL(ca.enabled,true) = true	
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ca.id,ca.asset_id ORDER BY ca.source_twin_is_deleted, ca.target_twin_is_deleted, ca._staged_at DESC) = 1
;

CREATE OR REPLACE TABLE transformed.sustainability_twins AS SELECT * FROM transformed.sustainability_twins_v;