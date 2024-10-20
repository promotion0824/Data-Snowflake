-- ******************************************************************************************************************************
-- Create view
-- ******************************************************************************************************************************

CREATE OR REPLACE VIEW transformed.sustainability_utility_bills_v AS

WITH cte_monthly_total AS (
    SELECT 
		DATE_TRUNC('MONTH',d.date) AS month_start_date,
		SUM(avg_daily_value) AS monthly_total_value,
		trend_id,
		external_id,
		billing_period_start,
		billing_period_end,
		count(*) as billing_period_days_in_month,
		MAX(last_captured_at_utc) AS last_captured_at_utc,
		MAX(last_refreshed_at_utc) AS last_refreshed_at_utc
    FROM transformed.sustainability_utility_bills_raw u
    JOIN utils.dates d ON d.date >= billing_period_start AND date < billing_period_end
    GROUP BY month_start_date, trend_id, external_id,billing_period_start,billing_period_end
)
,cte_consumption AS (
    SELECT DISTINCT
		ts.month_start_date,
		a.trend_id,
		ts.external_id,
		CASE UPPER(a.unit)
			WHEN 'WH' THEN 'kWh'
			WHEN 'MMBTU' THEN 'Therms'
			ELSE a.unit
		END AS unit,
		CASE UPPER(a.unit)
			WHEN 'WH'  THEN ts.monthly_total_value / 1000.0 
			WHEN 'MMBTU' THEN ts.monthly_total_value / 10.0
			ELSE ts.monthly_total_value
		END AS utility_billed_amount,
		CASE 
			WHEN service_type = 'Natural Gas' AND a.model_id NOT LIKE '%Cost%' THEN utility_billed_amount  * 100000 
			WHEN service_type = 'Electricity' AND a.model_id NOT LIKE '%Cost%' THEN utility_billed_amount * 3412.1416
			ELSE NULL
		END AS energy_consumption_BTU,
		energy_consumption_BTU/1000000 AS energy_consumption_mmBTU,
		ts.billing_period_start,
		ts.billing_period_end,
		billing_period_days_in_month,
		CONVERT_TIMEZONE( 'UTC', a.time_zone, MAX(ts.last_refreshed_at_utc) OVER ()) AS last_refreshed_at_local
    FROM cte_monthly_total ts
    JOIN transformed.sustainability_twins a ON (ts.external_id = a.external_id)
)
,cte_consumption_monthly AS (
    SELECT
		month_start_date,
		trend_id,
		external_id,
		SUM(utility_billed_amount) AS utility_billed_amount,
		SUM(energy_consumption_mmBTU) AS energy_consumption_mmBTU,
		SUM(billing_period_days_in_month) AS billing_period_days_in_billing_month
    FROM cte_consumption
	GROUP BY month_start_date, trend_id, external_id
)
SELECT DISTINCT
	ts.month_start_date,
	a.trend_id,
	a.capability_id AS dt_id,
	ts.external_id,
	ts.utility_billed_amount,
    ts.energy_consumption_BTU,
	ts.energy_consumption_mmBTU,
    --  how to pick the calendar month that has the most days in it to use the base month for comparison
	CASE WHEN billing_period_days_in_month >= 15 THEN m1.energy_consumption_mmBTU ELSE CAST(NULL AS FLOAT) END AS energy_consumption_mmBTU_previous_month,
	CASE WHEN billing_period_days_in_month >= 15 THEN y1.energy_consumption_mmBTU ELSE CAST(NULL AS FLOAT) END AS energy_consumption_mmBTU_previous_year,
	a.model_id,
	a.capability_id,
	a.capability_name,
	a.unit,
	a.service_type,
	a.asset_id,
	a.asset_name,
	a.provider_name,
    a.emissions_factor,
    a.emissions_factor_unit,
	ts.billing_period_start,
	ts.billing_period_end,
	a.site_id,
	a.site_name,
	a.time_zone,
	a.customer_id,
	a.portfolio_id,
	a.building_id,
	a.building_name,
	a.building_type,
	a.building_gross_area,
	a.building_rentable_area,
	a.building_gross_area_unit,
	ts.last_refreshed_at_local
FROM cte_consumption ts
JOIN transformed.sustainability_twins a ON (ts.external_id = a.external_id)
LEFT JOIN cte_consumption_monthly m1 ON DATEADD('month', -1,ts.month_start_date) = m1.month_start_date AND (ts.external_id = m1.external_id)
LEFT JOIN cte_consumption_monthly y1 ON DATEADD('month',-12,ts.month_start_date) = y1.month_start_date AND (ts.external_id = y1.external_id)
;

CREATE OR REPLACE TABLE transformed.sustainability_utility_bills AS SELECT * FROM transformed.sustainability_utility_bills_v;
