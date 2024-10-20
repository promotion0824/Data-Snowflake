-- ******************************************************************************************************************************
-- Stored procedure that persists view logic as tables for better performance
-- ******************************************************************************************************************************

CREATE OR REPLACE PROCEDURE transformed.sustainability_create_table_resources_comparison_sp()
    RETURNS STRING
    LANGUAGE SQL
    EXECUTE AS CALLER
  AS
    $$

	BEGIN

        CREATE OR REPLACE TABLE transformed.sustainability_resources_twins AS SELECT * FROM transformed.sustainability_resources_twins_v;

        CREATE OR REPLACE TABLE transformed.sustainability_resources_comparison_raw as
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
        WHERE date_local > '2022-06-01'
          AND (trend_id IN (select trend_id FROM transformed.sustainability_resources_twins)
           OR external_id IN (select external_id FROM transformed.sustainability_resources_twins WHERE external_id > ''))
        ;

      CREATE OR REPLACE TABLE transformed.sustainability_resources_comparison AS SELECT * FROM transformed.sustainability_resources_comparison_v;
	END
		$$
	;