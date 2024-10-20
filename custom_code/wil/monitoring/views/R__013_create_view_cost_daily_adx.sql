
CREATE OR REPLACE VIEW  central_monitoring_db.published.cost_daily_adx AS
WITH cte_costs AS (
    SELECT 
        subscription_name,
        resource_id,
        resource_group,
        consumed_service,
        meter_category AS category,
        product,
        service_family,
        customer_code,
        date,
        date_trunc(MONTH, date) AS month_start_date,
        IFNULL(environment,'nonprod') AS environment,
        TO_NUMBER(sum(cost_in_usd),14,5) AS raw_cost,
        tags,
        'cost_from_adx' AS cost_type
    FROM central_monitoring_db.published.costs_daily_resource_cost
    WHERE consumed_service = 'Microsoft.Kusto'
      AND cost_in_usd != 0
      AND customer_code IS NOT NULL AND customer_code != '' AND customer_code != 'shared'
    GROUP BY subscription_name, resource_group, consumed_service, resource_id, date, category, environment, product, service_family,customer_code,tags
)
,cte_compute AS (
SELECT 
    costdate,
    databasename,
    SPLIT_PART(resourceid, '/', 3) AS subscription,
    SPLIT_PART(resourceid, '/', 5) AS resource_group,
    CASE 
         WHEN databasename ilike 'twin-%' THEN LOWER(STRTOK(databasename,'-',2))
         WHEN resource_group IN ('prod-platformdata-aue', 'adx-demo-uat') AND LOWER(STRTOK(databasename,'-',4)) IS NOT NULL THEN LOWER(STRTOK(databasename,'-',4))
         WHEN resource_group IN ('nonprod-platformdata') AND LOWER(STRTOK(databasename,'-',3)) IS NOT NULL THEN LOWER(STRTOK(databasename,'-',3))
         ELSE LOWER(STRTOK(databasename,'-',1)) 
    END AS customer_abbreviation,
    percentagetotalcputime AS percentage,
    SPLIT_PART(resourceid, '/', -1) AS clusterName,
    resourceid
FROM azure_consumption.costs.daily_adx_compute AS compute
)
,cte_storage AS (
SELECT 
    costdate,
    databasename,
    SPLIT_PART(resourceid, '/', 3) AS subscription,
    SPLIT_PART(resourceid, '/', 5) AS resource_group,
    CASE 
         WHEN databasename ilike 'twin-%' THEN LOWER(STRTOK(databasename,'-',2))
         WHEN resource_group IN ('prod-platformdata-aue', 'adx-demo-uat') AND LOWER(STRTOK(databasename,'-',4)) IS NOT NULL THEN LOWER(STRTOK(databasename,'-',4))
         WHEN resource_group IN ('nonprod-platformdata') AND LOWER(STRTOK(databasename,'-',3)) IS NOT NULL THEN LOWER(STRTOK(databasename,'-',3))
         ELSE LOWER(STRTOK(databasename,'-',1)) 
    END AS customer_abbreviation,
    percentagetotalextentsize AS percentage,
    SPLIT_PART(resourceid, '/', -1) AS clusterName,
    resourceid
FROM azure_consumption.costs.daily_adx_storage AS storage
)
-- Compute costs
SELECT 
    cost.date,
    cost.month_start_date,
    bu.business_unit,
    cost.resource_group,
    cost.resource_id,
    cost.subscription_name,
    CASE WHEN mg.management_group IN ('Willow Twin Development') THEN 'Development'
        WHEN mg.management_group IN ('Willow Twin Production') THEN 'Production'
        WHEN cost.subscription_name IS NULL AND (product ILIKE 'Reserved VM%') THEN 'Production'
        WHEN cost.subscription_name IN ('K8S-INTERNAL') THEN 'Production'
        WHEN cost.subscription_name ILIKE ANY ('%prd%','%prod%','%uat%') OR cost.resource_group ILIKE ANY ('%prd%','%prod%','%uat%') THEN 'Production'
        ELSE 'Development'
    END AS workload_type,
    CASE WHEN cost.subscription_name IS NULL AND (product ILIKE 'Reserved VM%' OR service_family ILIKE 'SaaS%') THEN 'CoreIT'
    	 ELSE mg.management_group 
    END AS management_group, 
    cost.consumed_service,
    cost.product,
    cost.service_family,
    cost.environment,
    cost.customer_code,
    SUM(cost.raw_cost) AS total_cost,
    ANY_VALUE(compute.percentage) AS pct,
    TO_NUMBER(pct,20,5)/100 * total_cost AS cost,
    cost.tags,
    cost.cost_type
FROM cte_compute AS compute
JOIN cte_costs AS cost
     ON (cost.resource_id = compute.resourceid)
    AND (cost.date = compute.costdate)
    AND (cost.category NOT IN ('Storage','Bandwidth'))
LEFT JOIN azure_consumption.costs.customer_lookup cl
     ON cl.abbreviation = compute.customer_abbreviation
LEFT JOIN central_monitoring_db.transformed.management_groups mg 
     ON (cost.subscription_name = mg.subscription_name)
LEFT JOIN central_monitoring_db.transformed.business_units bu
     ON (mg.management_group = bu.management_group)
GROUP BY 
    cost.date,
    cost.month_start_date,
    mg.management_group,
    bu.business_unit,
    workload_type,
    cost.resource_group,
    cost.resource_id,
    cost.subscription_name,
    cost.consumed_service,
    cost.product,
    cost.service_family,
    cost.environment,
    cost.customer_code,
    cost.cost_type,
    cost.tags

UNION ALL
-- Storage costs
SELECT 
    cost.date,
    cost.month_start_date,
    bu.business_unit,
    cost.resource_group,
    cost.resource_id,
    cost.subscription_name,
    CASE WHEN mg.management_group IN ('Willow Twin Development') THEN 'Development'
         WHEN mg.management_group IN ('Willow Twin Production') THEN 'Production'
         WHEN cost.subscription_name IS NULL AND (product ILIKE 'Reserved VM%') THEN 'Production'
         WHEN cost.subscription_name IN ('K8S-INTERNAL') THEN 'Production'
         WHEN cost.subscription_name ILIKE ANY ('%prd%','%prod%','%uat%') OR cost.resource_group ILIKE ANY ('%prd%','%prod%','%uat%') THEN 'Production'
         ELSE 'Development'
    END AS workload_type,
    CASE WHEN cost.subscription_name IS NULL AND (product ILIKE 'Reserved VM%' OR service_family ILIKE 'SaaS%') THEN 'CoreIT'
    	 ELSE mg.management_group 
    END AS management_group, 
    cost.consumed_service,
    cost.product,
    cost.service_family,
    cost.environment,
    cost.customer_code,
    SUM(cost.raw_cost) AS total_cost,
    ANY_VALUE(storage.percentage) AS pct,
    TO_NUMBER(pct,20,5)/100 * total_cost AS cost,
    cost.tags,
    cost.cost_type
FROM cte_storage AS storage
JOIN cte_costs AS cost
     ON (cost.resource_id = storage.resourceid)
    AND (cost.date = storage.costdate)
    AND (cost.category IN ('Storage','Bandwidth'))
LEFT JOIN azure_consumption.costs.customer_lookup cl
     ON (cl.abbreviation = storage.customer_abbreviation)
LEFT JOIN central_monitoring_db.transformed.management_groups mg 
     ON (cost.subscription_name = mg.subscription_name)
LEFT JOIN central_monitoring_db.transformed.business_units bu
     ON (mg.management_group = bu.management_group)
GROUP BY 
    cost.date,
    cost.month_start_date,
    mg.management_group,
    bu.business_unit,
    cost.resource_group,
    cost.resource_id,
    cost.subscription_name,
    workload_type,
    cost.consumed_service,
    cost.product,
    cost.service_family,
    cost.environment,
    cost.customer_code,
    cost.tags,
    cost.cost_type
UNION ALL
-- pick up any costs for clusters that are not running;
SELECT 
    cost.date,
    cost.month_start_date,
    bu.business_unit,
    cost.resource_group,
    cost.resource_id,
    cost.subscription_name,
    CASE WHEN mg.management_group IN ('Willow Twin Development') THEN 'Development'
         WHEN mg.management_group IN ('Willow Twin Production') THEN 'Production'
         WHEN cost.subscription_name IN ('K8S-INTERNAL') THEN 'Production'
         WHEN cost.subscription_name ILIKE ANY ('%prd%','%prod%','%uat%') OR cost.resource_group ILIKE ANY ('%prd%','%prod%','%uat%') THEN 'Production'
         ELSE 'Development'
    END AS workload_type,
    CASE WHEN cost.subscription_name IS NULL AND (product ILIKE 'Reserved VM%' OR service_family ILIKE 'SaaS%') THEN 'CoreIT'
    	 ELSE mg.management_group 
    END AS management_group, 
    cost.consumed_service,
    cost.product,
    cost.service_family,
    cost.environment,
    cost.customer_code,
    SUM(cost.raw_cost) AS total_cost,
    NULL AS pct,
    SUM(cost.raw_cost) AS cost,
    cost.tags,
    cost.cost_type
FROM cte_costs cost
LEFT JOIN azure_consumption.costs.customer_lookup cl
     ON (cl.abbreviation = cost.customer_code)
LEFT JOIN central_monitoring_db.transformed.management_groups mg 
     ON (cost.subscription_name = mg.subscription_name)
LEFT JOIN central_monitoring_db.transformed.business_units bu
     ON (mg.management_group = bu.management_group)
WHERE NOT EXISTS (SELECT 1 FROM cte_storage storage WHERE cost.resource_id = storage.resourceid AND cost.date = storage.costdate)
  AND NOT EXISTS (SELECT 1 FROM cte_compute compute WHERE cost.resource_id = compute.resourceid AND cost.date = compute.costdate)
  GROUP BY 
    cost.date,
    cost.month_start_date,
    mg.management_group,
    bu.business_unit,
    cost.resource_group,
    cost.resource_id,
    cost.subscription_name,
    workload_type,
    cost.consumed_service,
    cost.product,
    cost.service_family,
    cost.environment,
    cost.customer_code,
    cost.cost_type,
    cost.tags
;