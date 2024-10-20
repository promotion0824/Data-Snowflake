-- ******************************************************************************************************************************
-- Create view
-- ******************************************************************************************************************************

CREATE OR REPLACE VIEW transformed.sustainability_resources_comparison_v AS
WITH cte_monthly_total AS (
    SELECT 
		DATE_TRUNC('MONTH',d.date) AS month_start_date,
		rt.building_id,
		rt.building_name,
		rt.resource_type,
		rt.data_type,
		SUM(CASE WHEN rt.model_id     ILIKE '%Cost%' THEN avg_daily_value ELSE NULL END) AS cost,
		MAX(CASE WHEN rt.resource_type = 'Electricity' THEN 'MWh'
			 WHEN rt.resource_type = 'Natural Gas' THEN 'MBtu'
			 WHEN rt.resource_type = 'Steam' THEN 'klbs'
			 WHEN rt.resource_type ILIKE '%Water%' AND rt.model_id NOT ILIKE '%Cost%' THEN rt.unit
			 ELSE NULL
		END) AS conformed_unit,
		SUM(CASE WHEN rt.resource_type = 'Electricity' AND rt.unit ILIKE 'MWh' THEN avg_daily_value
		         WHEN rt.resource_type = 'Electricity' AND rt.unit ILIKE 'kWh' THEN avg_daily_value / 1000
		         WHEN rt.resource_type = 'Electricity' AND rt.unit ILIKE 'Wh' THEN avg_daily_value / 1000000
			     WHEN rt.resource_type = 'Natural Gas' AND rt.unit ILIKE '%MBtu' THEN avg_daily_value
				 WHEN rt.resource_type = 'Natural Gas' AND rt.unit ILIKE '%MWh' THEN avg_daily_value * 3.412142
			     WHEN rt.resource_type = 'Natural Gas' AND rt.unit ILIKE 'Therm%' THEN avg_daily_value * .1
			     WHEN rt.resource_type = 'Natural Gas' AND rt.unit ILIKE 'CCF' THEN avg_daily_value * .1026
				 WHEN rt.resource_type = 'Natural Gas' AND rt.unit ILIKE ANY('SCF','CF','ft3') THEN avg_daily_value * .001026
			     WHEN rt.resource_type = 'Steam' AND rt.unit ILIKE 'klbs' THEN avg_daily_value
			     WHEN rt.resource_type = 'Steam' AND rt.unit ILIKE 'lb' THEN avg_daily_value / 1000
			     WHEN rt.resource_type LIKE '%Water%' AND rt.model_id NOT ILIKE '%Cost%' THEN avg_daily_value
			ELSE NULL 
		END) AS amount,
		'MBtu' AS energy_unit,
		CASE WHEN rt.resource_type = 'Electricity' THEN amount * 3.412142
			 WHEN rt.resource_type = 'Natural Gas' THEN amount
			 WHEN rt.resource_type = 'Steam' THEN amount * 1.194
			 WHEN rt.resource_type LIKE '%Water%' THEN NULL
			 ELSE NULL
		END AS energy_consumption,
		MAX(CASE WHEN rt.model_id ILIKE '%Cost%' THEN rt.unit ELSE NULL END) AS cost_unit,
		emissions_factor_unit,
		MAX(emissions_factor) AS emissions_factor,
		emissions_scope,
		MAX(rt.time_zone) AS time_zone,
		rt.customer_id,
		rt.portfolio_id,
		rt.property_type,
		rt.building_gross_area,
		rt.building_rentable_area,
		rt.building_gross_area_unit,
		rt.property_year_built,
		rt.building_region,
		rt.city,
		MAX(last_captured_at_utc) AS last_captured_at_utc,
		MAX(last_refreshed_at_utc) AS last_refreshed_at_utc
    FROM transformed.sustainability_resources_comparison_raw ts
    JOIN utils.dates d 
	  ON d.date >= billing_period_start 
	 AND date < billing_period_end
	JOIN transformed.sustainability_resources_twins rt 
	  ON (ts.trend_id = rt.trend_id OR ts.external_id = rt.external_id)
	WHERE rt.data_type = 'Metered'
    GROUP BY month_start_date, rt.building_id, rt.building_name, resource_type, data_type, emissions_factor_unit, emissions_scope,
		rt.customer_id,
		rt.portfolio_id,
		rt.property_type,
		rt.building_gross_area,
		rt.building_rentable_area,
		rt.building_gross_area_unit,
		rt.property_year_built,
		rt.building_region,
		rt.city
)
, cte_consumption AS (
    SELECT DISTINCT
	ts.month_start_date,
	ts.resource_type,
	ts.data_type,
	ts.building_id,
	ts.building_name,
	ts.cost,
	ts.amount,
	ts.conformed_unit,
	ts.emissions_factor,
	ts.emissions_factor_unit,
	ts.emissions_scope,
	ts.energy_consumption,
	ts.energy_unit,
	CASE WHEN resource_type = 'Electricity' THEN ts.amount * 1000 * emissions_factor
			WHEN resource_type = 'Natural Gas' THEN ts.energy_consumption * emissions_factor
			WHEN resource_type = 'Steam' THEN NULL
			ELSE NULL
	END AS carbon_emissions,
	ts.customer_id,
	ts.portfolio_id,
	ts.property_type,
	ts.building_gross_area,
	ts.building_rentable_area,
	ts.building_gross_area_unit,
	ts.property_year_built,
	ts.building_region,
	ts.city,
	CONVERT_TIMEZONE( 'UTC', time_zone, MAX(ts.last_refreshed_at_utc) OVER ()) AS last_refreshed_at_local
    FROM cte_monthly_total ts
)
SELECT DISTINCT
	c.month_start_date,
	c.cost,
	c.amount,
	c.conformed_unit,
	c.energy_consumption,
	c.energy_unit,
    m1.energy_consumption AS energy_consumption_previous_month,
    y1.energy_consumption AS energy_consumption_previous_year,
	c.resource_type,
	c.data_type,
	c.emissions_factor,
	c.emissions_factor_unit,
	c.emissions_scope,
	c.carbon_emissions,
	c.customer_id,
	c.portfolio_id,
	c.building_id,
	c.building_name,
	c.property_type,
	c.building_gross_area,
	c.building_rentable_area,
	c.building_gross_area_unit,
	c.property_year_built,
	c.building_region,
	c.city,
	c.last_refreshed_at_local
FROM cte_consumption c
LEFT JOIN cte_consumption m1 ON DATEADD('month', -1,c.month_start_date) = m1.month_start_date 
							AND (c.building_id = m1.building_id)
							AND (c.resource_type = m1.resource_type)
							AND (c.data_type = m1.data_type)
LEFT JOIN cte_consumption y1 ON DATEADD('month',-12,c.month_start_date) = y1.month_start_date 
							AND (c.building_id = y1.building_id)
							AND (c.resource_type = y1.resource_type)
							AND (c.data_type = y1.data_type)
;

CREATE OR REPLACE TABLE transformed.sustainability_resources_comparison AS SELECT * FROM transformed.sustainability_resources_comparison_v;
