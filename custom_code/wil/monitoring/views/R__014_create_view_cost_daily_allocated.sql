
CREATE OR REPLACE VIEW central_monitoring_db.published.cost_daily_allocated_to_customer
COMMENT = 'Customer daily costs, with shared costs allocated across all customers in a subscription based on row counts'
AS
WITH cte_pct_total AS (
    SELECT 
        adx.customer_code,
        adx.month_start_date,
        RATIO_TO_REPORT(SUM(adx.totalcost)) OVER (PARTITION BY adx.month_start_date) as cust_pct_total
    FROM central_monitoring_db.published.daily_adx_costs adx
    LEFT JOIN azure_consumption.costs.customer_lookup cl
        ON adx.customer_code = cl.abbreviation
    WHERE subscription_name NOT IN ('Rail-UAT', 'Rail-PRE', 'Rail-PRD', 'Rail-DEV', 'Rail-DEMO','Mining-POC')
      AND adx.customer_code NOT IN ('mining','ncr','skt','wrt','bhp','fmg','shared')
      AND adx.environment = 'prod'
    GROUP BY adx.customer_code, adx.month_start_date
)
, cost_for_allocation AS (
    SELECT 
    date,
    date_trunc(MONTH, date) AS month_start_date,
    resource_group,resource_id,
    subscription_name,
    consumed_service,
    product,
    service_family,
    environment,
    SUM(cost_in_usd) AS raw_cost,
    'cost_allocated_to_customer' AS cost_type,
    tags
    FROM central_monitoring_db.published.costs_daily_resource_cost
    WHERE (subscription_name NOT IN ('Rail-UAT', 'Rail-PRE', 'Rail-PRD', 'Rail-DEV', 'Rail-DEMO','Mining-POC') OR subscription_name IS NULL)
    AND (consumed_service != 'Microsoft.Kusto' OR consumed_service IS NULL OR customer_code IS NULL OR customer_code = '' OR customer_code = 'shared')
    AND COALESCE (
            IFF(subscription_name = 'Experience-PRD','inv',NULL),
            IFF(lower(strtok(resource_group,'-',4)) = 'lda', strtok(resource_group,'-',5), NULL),
            IFF(lower(strtok(resource_group,'-',1)) = 'wdt', strtok(resource_group,'-',3), NULL),
            customer_abbreviation,
            'shared'
    )  NOT IN (SELECT DISTINCT customer_code FROM cte_pct_total)
    AND cost_in_usd != 0
    GROUP BY date, month_start_date, resource_group, subscription_name, consumed_service,product, service_family, environment,resource_id,tags
)
SELECT 
    cost.date,
    cost.month_start_date,
    CASE 
    	WHEN cost.subscription_name IS NULL AND (product ILIKE 'Reserved VM%' OR service_family ILIKE 'SaaS%') THEN 'IT'
    	WHEN cost.subscription_name IS NULL AND (service_family ILIKE 'Azure Marketplace Services') THEN 'IT'
    	ELSE bu.business_unit 
    END AS business_unit,
    CASE WHEN mg.management_group IN ('Willow Twin Development') THEN 'Development'
         WHEN mg.management_group IN ('Willow Twin Production') THEN 'Production'
         WHEN cost.subscription_name IS NULL AND (product ILIKE 'Reserved VM%') THEN 'Production'
         WHEN cost.subscription_name ILIKE ANY ('%prd%','%prod%','%uat%') OR cost.resource_group ILIKE ANY ('%prd%','%prod%','%uat%') THEN 'Production'
         WHEN cost.subscription_name IN ('K8S-INTERNAL') THEN 'Production'
         ELSE 'Development'
    END AS workload_type,
    CASE WHEN cost.subscription_name IS NULL AND (product ILIKE 'Reserved VM%' OR service_family ILIKE 'SaaS%') THEN 'CoreIT'
    	 ELSE bu.management_group 
    END AS management_group, 
    cost.resource_group,
    cost.resource_id,
    cost.subscription_name,
    cost.consumed_service,
    cost.product,
    cost.service_family,
    cost.environment,
    pct.customer_code AS customer_code,
    cost.raw_cost,
    pct.cust_pct_total,
    TO_NUMBER(cost.raw_cost * pct.cust_pct_total, 18, 10) AS cust_allocated_cost,
    cost_type,
    tags
  FROM cost_for_allocation AS cost
  LEFT JOIN cte_pct_total pct ON cost.month_start_date = pct.month_start_date
  LEFT JOIN central_monitoring_db.transformed.management_groups mg 
    ON (cost.subscription_name = mg.subscription_name)
  LEFT JOIN central_monitoring_db.transformed.business_units bu
    ON (mg.management_group = bu.management_group)
;