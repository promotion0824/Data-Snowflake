CREATE OR REPLACE VIEW central_monitoring_db.published.cost_by_resource_group AS

    SELECT 
        month_start_date,
        management_group,
        business_unit,
        subscription_name,
        consumed_service,
        product,
        resource_group,
        customer_code,
        workload_type,
        cost_type,
        sum(raw_cost) AS cost,
        tags
    FROM central_monitoring_db.published.cost_daily_by_customer
    WHERE month_start_date >= '2022-01-01'   
    GROUP BY 
        month_start_date,
        management_group,
        business_unit,
        subscription_name,
        consumed_service,
        product,
        resource_group,
        customer_code,
        workload_type,
        cost_type,
        tags

    UNION ALL

    SELECT 
        month_start_date,
        management_group,
        business_unit,
        subscription_name,
        consumed_service,
        product,
        resource_group,
        customer_code,
        workload_type,
        cost_type,
        sum(cust_allocated_cost),
        tags
    FROM central_monitoring_db.published.cost_daily_allocated_to_customer
    WHERE month_start_date >= '2022-01-01'  
    GROUP BY    
        month_start_date,
        management_group,
        business_unit,
        subscription_name,
        consumed_service,
        product,
        resource_group,
        customer_code,
        workload_type,
        cost_type,
        tags

    UNION ALL 

    SELECT 
        month_start_date,
        management_group,
        business_unit,
        subscription_name,
        consumed_service,
        product,
        resource_group,
        customer_code,
        workload_type,
        cost_type,
        sum(cost),
        tags
    FROM central_monitoring_db.published.cost_daily_adx
    WHERE month_start_date >= '2022-01-01'
    GROUP BY 
        month_start_date,
        management_group,
        business_unit,
        subscription_name,
        consumed_service,
        product,
        resource_group,
        customer_code,
        workload_type,
        cost_type,
        tags

-- the data already contains log analtyics costs
    -- UNION ALL 

    -- SELECT 
    --     month_start_date,
    --     management_group,
    --     business_unit,
    --     subscription_name,
    --     consumed_service,
    --     product,
    --     resource_group,
    --     customer_code,
    --     sum(allocated_cost)
    -- FROM central_monitoring_db.published.costs_log_analytics
    -- WHERE month_start_date >= '2022-01-01'
    -- GROUP BY 
    --     month_start_date,
    --     management_group,
    --     business_unit,
    --     subscription_name,
    --     consumed_service,
    --     product,
    --     resource_group,
    --     customer_code
;
