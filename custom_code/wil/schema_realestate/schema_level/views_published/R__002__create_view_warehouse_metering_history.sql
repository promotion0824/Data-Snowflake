-- ------------------------------------------------------------------------------------------------------------------------------
-- monitoring view
-- ------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW published.warehouse_metering_history AS
SELECT
    organization_name,
    account_name,
    region,
    service_type,
    DATE_TRUNC('MONTH', start_time) AS month,
    DATE_TRUNC('Day', start_time) AS day,
    HOUR(start_time) AS hour_of_day,
	DAYNAME(start_time) as day_of_week,
	start_time,
	end_time,
	DATEDIFF('s', start_time, end_time) AS duration_seconds,
    warehouse_id,
    warehouse_name,
    credits_used,
    credits_used_compute,
    credits_used_cloud_services,
    account_locator
FROM  snowflake.organization_usage.warehouse_metering_history;