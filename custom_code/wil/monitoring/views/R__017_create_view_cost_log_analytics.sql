-- ******************************************************************************************************************************
-- Create view
-- ******************************************************************************************************************************

CREATE OR REPLACE VIEW central_monitoring_db.published.costs_log_analytics AS
SELECT -- WE NEED TO GET THIS AT THE CUSTOMER LEVEL
    costdate AS date, 
    date_trunc(MONTH, date) AS month_start_date,
    dla.application,
    SPLIT_PART(dla.resourceid, '/', -1) AS resource_tail,
    SPLIT_PART(dla.resourceid, '/', 5) AS resource_group_parsed,
    SPLIT_PART(lk.resourceid, '/', -1) AS loganalytics_workspace,
    SUM(cost.cost_in_usd) workspace_cost,
    SUM(percentagetotal) pctTotal,
    TO_NUMBER(workspace_cost,20,10) * (TO_NUMBER(pctTotal,15,12)/100) AS allocated_cost, 
    SUM(resourcebilledsize)/1000000 billedSize_in_MiB, 
    COALESCE(cost.customer_code, cost.customer_abbreviation) AS cust_code,
    dla.resourceid AS resource_id,
    mg.management_group,
    bu.business_unit,
    CASE WHEN cost.environment IN ('prod') THEN 'Production' ELSE 'Development' END AS workload_type,
    cost.resource_group,
    cost.subscription_name,
    cost.consumed_service,
    cost.product,
    cost.service_family,
    cost.customer_code,
    'log analytics' AS cost_type
FROM azure_consumption.costs.daily_log_analytics dla
JOIN azure_consumption.costs.log_analytics_workspace_lookup lk ON lk.workspaceid = dla.workspaceid
JOIN central_monitoring_db.published.costs_daily_resource_cost cost ON lk.resourceid = cost.resource_Id AND dla.costdate = cost.date
-- LEFT JOIN azure_consumption.costs.customer_lookup cl
--      ON cl.abbreviation = compute.customer_abbreviation
LEFT JOIN central_monitoring_db.transformed.management_groups mg 
     ON (cost.subscription_name = mg.subscription_name)
LEFT JOIN central_monitoring_db.transformed.business_units bu
     ON (mg.management_group = bu.management_group)
GROUP BY
    dla.costdate,
    month_start_date,
    dla.application,
    mg.management_group,
    bu.business_unit,
    workload_type,
    dla.resourceid,
    loganalytics_workspace,
    resource_tail,
    resource_group_parsed,
    cost.resource_group,
    cost.subscription_name,
    cost.consumed_service,
    cost.product,
    cost.service_family,
    cost.customer_code,
    cost.environment,
    cust_code,
    cost_type
