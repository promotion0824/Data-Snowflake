-- ******************************************************************************************************************************
-- Create view
-- ******************************************************************************************************************************
CREATE OR REPLACE VIEW published.sustainability_ghg_emissions AS

SELECT
	ts.month_start_date,
	ts.trend_id,
	ts.dt_id,
	ts.utility_billed_amount,
	ts.unit,
	CASE WHEN ts.model_id NOT LIKE '%Cost%' THEN ts.energy_consumption_BTU ELSE NULL END AS energy_consumption_BTU,
	CASE WHEN ts.model_id NOT LIKE '%Cost%' THEN ts.energy_consumption_mmBTU ELSE NULL END AS energy_consumption_mmBTU,
	CASE WHEN ts.service_type = 'Electricity' AND ts.model_id NOT LIKE '%Cost%' THEN 'Scope 2' 
	     WHEN ts.service_type = 'Natural Gas' AND ts.model_id NOT LIKE '%Cost%' THEN 'Scope 1'
	ELSE NULL 
	END AS scope,
	CASE    WHEN ts.model_id NOT LIKE '%Cost%' THEN ts.emissions_factor 
            ELSE NULL END AS emissions_factor,
	CASE    WHEN ts.model_id NOT LIKE '%Cost%' THEN ts.emissions_factor_unit
			ELSE NULL END AS emissions_factor_unit,
	CASE    WHEN ts.service_type = 'Electricity' AND ts.model_id NOT LIKE '%Cost%' THEN utility_billed_amount * ts.emissions_factor 
			WHEN ts.service_type = 'Natural Gas' AND ts.model_id NOT LIKE '%Cost%'  THEN energy_consumption_mmBTU  * ts.emissions_factor
			ELSE NULL 
	END AS ghg_emissions,
	ts.service_type AS energy_source,
	ts.service_type,
	ts.model_id,
	ts.capability_id,
	ts.capability_name,
	ts.asset_id,
	ts.asset_name,
	ts.provider_name,
	ts.site_id,
	ts.site_name,
	ts.time_zone,
	ts.customer_id,
	ts.portfolio_id,
	ts.building_id,
	ts.building_name,
	ts.building_type,
	ts.building_gross_area,
	ts.building_gross_area_unit,
	ts.building_rentable_area,
	ts.last_refreshed_at_local
FROM transformed.sustainability_utility_bills ts
WHERE ts.service_type IN ('Electricity', 'Natural Gas')
;