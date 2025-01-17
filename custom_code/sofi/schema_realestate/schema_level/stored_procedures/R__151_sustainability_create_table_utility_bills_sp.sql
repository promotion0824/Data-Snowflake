-- ******************************************************************************************************************************
-- Stored procedure that persists view logic as tables for better performance
-- ******************************************************************************************************************************

CREATE OR REPLACE PROCEDURE transformed.sustainability_create_table_utility_bills_sp()
    RETURNS STRING
    LANGUAGE SQL
    EXECUTE AS CALLER
  AS
    $$

	BEGIN

        CREATE OR REPLACE TABLE transformed.sustainability_utility_bills_raw as
        SELECT DISTINCT
            lag(timestamp_utc) OVER (PARTITION BY trend_id ORDER BY timestamp_utc)  AS billing_period_start,
            timestamp_utc AS billing_period_end,
            DATEDIFF(d, billing_period_start, billing_period_end) AS days_in_period,
            telemetry_value,
            telemetry_value / CASE WHEN IFNULL(days_in_period,0) = 0 THEN 1 ELSE days_in_period END AS avg_daily_value,
            trend_id,
            NULLIF(external_id,'') AS external_id,
            dt_id,
            (timestamp_utc) AS last_captured_at_utc,
            (SYSDATE()) AS last_refreshed_at_utc
        FROM transformed.telemetry ts
        WHERE trend_id IN (select trend_id FROM transformed.sustainability_twins)
           OR external_id IN (select external_id FROM transformed.sustainability_twins WHERE external_id > '')

      UNION ALL

        SELECT DISTINCT
            periodstart AS billing_period_start,
            periodend AS billing_period_end,
            DATEDIFF(d, billing_period_start, billing_period_end) AS days_in_period,
            consumption,
            consumption / CASE WHEN IFNULL(days_in_period,0) = 0 THEN 1 ELSE days_in_period END AS avg_daily_value,
            'e7c49162-1867-49d4-9cde-6313f5ee8fa8' AS trend_id,
            NULL AS external_id,
            NULL dt_id,
            ('2024-05-03') AS last_captured_at_utc,
            (SYSDATE()) AS last_refreshed_at_utc
        FROM transformed.utility_data_historical

      UNION ALL

        SELECT DISTINCT
            periodstart AS billing_period_start,
            periodend AS billing_period_end,
            DATEDIFF(d, billing_period_start, billing_period_end) AS days_in_period,
            consumptioncost,
            consumptioncost / CASE WHEN IFNULL(days_in_period,0) = 0 THEN 1 ELSE days_in_period END AS avg_daily_value,
            '5fca5e7a-85d3-4865-bf69-19b44cabf7ef' AS trend_id,
            NULL AS external_id,
            NULL dt_id,
            ('2024-05-03') AS last_captured_at_utc,
            (SYSDATE()) AS last_refreshed_at_utc
        FROM transformed.utility_data_historical
        ;

        CREATE OR REPLACE TABLE transformed.sustainability_utility_bills AS SELECT * FROM transformed.sustainability_utility_bills_v;
	END
		$$
	;

CALL transformed.sustainability_create_table_utility_bills_sp();