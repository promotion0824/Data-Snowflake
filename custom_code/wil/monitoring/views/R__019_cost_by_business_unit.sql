CREATE OR REPLACE VIEW central_monitoring_db.published.cost_by_business_unit AS
WITH cte_cost AS (
    SELECT 
        month_start_date,
        management_group,
        business_unit,
        subscription_name,
        workload_type,
        customer_code,
        sum(raw_cost) AS cost,
        cost_type
    FROM central_monitoring_db.published.cost_daily_by_customer
    WHERE month_start_date >= '2022-01-01'
      AND (customer_code NOT IN ('mining','ncr','skt','wrt','bhp','fmg')
       OR  customer_code IS NULL)
    GROUP BY 
        month_start_date,
        management_group,
        business_unit,
        subscription_name,
        workload_type,
        customer_code,
        cost_type
        
    UNION ALL

    SELECT 
        month_start_date,
        management_group,
        business_unit,
        subscription_name,
        workload_type,
        customer_code,
        sum(cust_allocated_cost),
        cost_type
    FROM central_monitoring_db.published.cost_daily_allocated_to_customer
    WHERE month_start_date >= '2022-01-01'
      AND (customer_code NOT IN ('mining','ncr','skt','wrt','bhp','fmg')
       OR  customer_code IS NULL)
    GROUP BY    
        month_start_date,
        management_group,
        business_unit,
        subscription_name,
        workload_type,
        customer_code,
        cost_type

    UNION ALL 

    SELECT 
        month_start_date,
        management_group,
        business_unit,
        subscription_name,
        workload_type,
        customer_code,
        sum(cost),
        cost_type
    FROM central_monitoring_db.published.cost_daily_adx
    WHERE month_start_date >= '2022-01-01'
      AND (customer_code NOT IN ('mining','ncr','skt','wrt','bhp','fmg')
       OR  customer_code IS NULL)
    GROUP BY 
        month_start_date,
        management_group,
        business_unit,
        subscription_name,
        workload_type,
        customer_code,
        cost_type
)
SELECT 
    month_start_date,
    management_group,
    business_unit,
    subscription_name,
    workload_type,
    cl.name AS customer_name,
    c.customer_code,
    ROUND(cost,2) AS cost,
    cost_type
FROM cte_cost c
LEFT JOIN azure_consumption.costs.customer_lookup cl
    ON (c.customer_code = cl.abbreviation)
;
