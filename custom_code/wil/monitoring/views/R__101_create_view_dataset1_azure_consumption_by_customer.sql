-- dataset1_azure_consumption_by_customer

CREATE OR REPLACE VIEW central_monitoring_db.published.dataset1_azure_consumption_by_customer AS
WITH cte_costs AS (
SELECT 
    month_start_date,
    customername,
    UPPER(customer_code) AS customer_code,
    CASE WHEN customerName IS NULL THEN customer_abbreviation ELSE NULL END AS rsg_abbreviation,
    SUM(fullyallocateditemcost) AS cost_usd
FROM central_monitoring_db.published.dataset_allocated_details
GROUP BY month_start_date, customername, customer_code, rsg_abbreviation
)
, cte_volume AS (
SELECT 
    month_start_date,
    UPPER(customer_abbreviation) AS customer_code,
    COUNT(DISTINCT site_id) AS number_of_buildings
FROM central_monitoring_db.published.site_volume_by_month
GROUP BY month_start_date, customer_code
)
SELECT 
c.month_start_date, 'Subscriptions' AS cost_type,
c.customerName,
COALESCE(c.customer_code,c.rsg_abbreviation) AS customer_code,
SUM(c.cost_usd) AS cost_allocated,
max(sv.number_of_buildings) AS number_of_buildings
FROM cte_costs c
LEFT JOIN cte_volume sv
  ON (c.month_start_date = sv.month_start_date)
 AND (c.customer_code = sv.customer_code)
GROUP BY c.month_start_date, c.customerName, COALESCE(c.customer_code,c.rsg_abbreviation)

UNION ALL

SELECT 
adx.month_start_date, 'ADX' AS cost_type,
adx.customer_name,
adx.customer_code,
SUM(adx.dailycost) AS dailycost,
max(sv.number_of_buildings) AS number_of_buildings
FROM  central_monitoring_db.published.daily_adx_costs adx
LEFT JOIN cte_volume sv
  ON (adx.month_start_date = sv.month_start_date)
 AND (adx.customer_code = sv.customer_code)
GROUP BY adx.month_start_date, adx.customer_name, adx.customer_code
ORDER BY month_start_date DESC, customerName;