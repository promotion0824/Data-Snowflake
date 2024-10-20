-- ------------------------------------------------------------------------------------------------------------------------------
-- monitoring view
-- ------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW central_monitoring_db.published.serverless_tasks_history_dashboard AS
SELECT 
	customer_identifier || '-' || account_name AS customer_account,
	region,
    task_name,
    date_trunc('MONTH', start_time) AS month,
    date_trunc('Day', start_time) AS day,
    HOUR(start_time) AS hour_of_day,
	DAYNAME(start_time) as day_of_week,
	start_time,
	end_time,
	DATEDIFF('s', start_time, end_time) AS duration_seconds,
    credits_used
FROM central_monitoring_db.published.serverless_tasks_history
;