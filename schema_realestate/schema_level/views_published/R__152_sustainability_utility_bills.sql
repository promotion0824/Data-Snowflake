-- ******************************************************************************************************************************
-- Create view
-- ******************************************************************************************************************************

CREATE OR REPLACE VIEW published.sustainability_utility_bills AS

SELECT
	ts.month_start_date,
	ts.trend_id,
	ts.external_id,
	ts.dt_id,
	ts.billing_period_start,
	ts.billing_period_end,
	ts.utility_billed_amount,
	ts.energy_consumption_BTU,
	ts.energy_consumption_mmBTU,
	ts.energy_consumption_mmBTU_previous_month,
	ts.energy_consumption_mmBTU_previous_year,
	ts.unit,
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
;