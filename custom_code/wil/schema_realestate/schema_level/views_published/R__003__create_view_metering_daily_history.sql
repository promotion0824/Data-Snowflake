-- ------------------------------------------------------------------------------------------------------------------------------
-- monitoring view
-- ------------------------------------------------------------------------------------------------------------------------------
CREATE VIEW published.metering_daily_history AS
SELECT
	service_type,
	organization_name,
	account_name,
	usage_date,
	credits_used_compute,
	credits_used_cloud_services,
	credits_used,
	credits_adjustment_cloud_services,
	credits_billed,
	region,
	account_locator
FROM snowflake.organization_usage.metering_daily_history;