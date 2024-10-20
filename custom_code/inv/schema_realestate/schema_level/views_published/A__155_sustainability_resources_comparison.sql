-- ******************************************************************************************************************************
-- Create view
-- ******************************************************************************************************************************


CREATE OR REPLACE VIEW published.sustainability_resources_comparison AS
SELECT DISTINCT
	month_start_date,
	cost,
	amount,
	conformed_unit,
	energy_consumption AS energy_used,
	energy_unit,
    energy_consumption_previous_month AS energy_used_previous_month,
    energy_consumption_previous_year AS energy_used_previous_year,
	resource_type,
	emissions_scope,
	data_type,
	carbon_emissions,
	emissions_factor_unit,
	customer_id,
	portfolio_id,
	building_id,
	building_name,
	property_type,
	building_gross_area,
	building_rentable_area,
	building_gross_area_unit,
	property_year_built,
	building_region,
	city,
	last_refreshed_at_local
FROM transformed.sustainability_resources_comparison;
