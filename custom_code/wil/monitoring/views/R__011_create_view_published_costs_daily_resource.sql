-- ******************************************************************************************************************************
-- Create view
-- ******************************************************************************************************************************
CREATE OR REPLACE VIEW central_monitoring_db.published.costs_daily_resource_cost AS
SELECT
    date,
    subscription_name,
    service_family,
    product,
    resource_group,
    resource_id,
	resource_location,
    location,
    meter_category,
    meter_subcategory,
	meter_region,
    cost_in_usd,
	consumption_begin_time,
	consumption_end_time,
    consumed_service,
    COALESCE(tags:application::STRING,tags:app::STRING) AS application,
    COALESCE(tags:"customer-code"::STRING, tags:"customer"::STRING) AS customer_code,
    tags:environment::STRING as environment,
    tags:stamp::STRING as stamp,
	tags,
	additional_info,
	additional_info_provider,
    CASE customer
        WHEN 'investor' THEN 'inv'
        ELSE COALESCE(tags:"customer-code"::STRING, customer)
    END AS customer_abbreviation,
	database_name
FROM azure_consumption.costs.daily_resource_cost c
WHERE cost_in_usd != 0
;
