-- ******************************************************************************************************************************
-- Create view 
-- ******************************************************************************************************************************

CREATE OR REPLACE VIEW published.sustainability_ghg_emissions_tou AS

SELECT
	ts.month_start_date,
	ts.trend_id,
	ts.energy_consumption,
	ts.unit,
	ts.energy_consumption_BTU,
	ts.energy_consumption_BTU/1000000 AS energy_consumption_mmBTU,
	ts.scope,
	ts.ghg_emissions_tou,
    ts.ghg_emissions_flat,
    ts.emissions_factor_tou,
    ts.emissions_factor_flat,
	ts.emissions_factor_source,
	ts.energy_source,
	ts.service_type,
	ts.model_id,
	ts.capability_id,
	ts.capability_name,
	ts.asset_id,
	ts.asset_name,
	ts.customer_id,
	ts.portfolio_id,
	ts.building_id,
	ts.building_name,
	ts.building_type,
	ts.building_gross_area,
	ts.building_gross_area_unit,
	ts.building_rentable_area,
	ts.last_refreshed_at_local
FROM transformed.sustainability_tou_electrical_emissions ts

UNION ALL

SELECT
	ts.month_start_date,
	ts.trend_id,
	ts.utility_billed_amount,
	ts.unit,
	ts.energy_consumption_BTU,
	ts.energy_consumption_mmBTU,
	ts.scope,
	ts.ghg_emissions AS ghg_emissions_tou,
	ts.ghg_emissions AS ghg_emissions_flat,   
    ts.ghg_emissions / utility_billed_amount AS emissions_factor_tou,
    ts.ghg_emissions / utility_billed_amount AS emissions_factor_flat,
	CASE WHEN ts.service_type = 'Natural Gas' THEN 'US EIA' 
	     WHEN ts.service_type = 'Steam' THEN 'eGrid 2021, US EPA' 
	     ELSE NULL 
	END AS emissions_factor_source,
	ts.energy_source,
	ts.service_type,
	ts.model_id,
	ts.capability_id,
	ts.capability_name,
	ts.asset_id,
	ts.asset_name,
	ts.customer_id,
	ts.portfolio_id,
	ts.building_id,
	ts.building_name,
	ts.building_type,
	ts.building_gross_area,
	ts.building_gross_area_unit,
	ts.building_rentable_area,
	ts.last_refreshed_at_local
FROM published.sustainability_ghg_emissions ts
WHERE ts.service_type NOT IN ('Electricity')
;