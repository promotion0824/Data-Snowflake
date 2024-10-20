-- ------------------------------------------------------------------------------------------------------------------------------
-- Clone from PRD to UAT
-- ------------------------------------------------------------------------------------------------------------------------------


CREATE OR REPLACE TASK transformed.create_table_hourly_temperature_trigger_tk
USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
--  At 10 minutes past every 8th hour
SCHEDULE = 'USING CRON 11 */3 * * * UTC'
SUSPEND_TASK_AFTER_NUM_FAILURES = 3
USER_TASK_TIMEOUT_MS = 3600000
ERROR_INTEGRATION = error_{{environment}}_nin
AS 
CREATE OR REPLACE TABLE transformed.sites_hourly_temperature AS

    SELECT 
        ts.date_local AS date,
        DATE_TRUNC('HOUR',ts.timestamp_local) AS date_hour,
        d.is_weekday,
        IFF(HOUR(date_hour) >= COALESCE(working_hours.default_value:hourStart,8) 
        AND HOUR(date_hour) <  COALESCE(working_hours.default_value:hourEnd, 18), true, false) AS is_working_hour,
        AVG(telemetry_value) AS temperature,
        ca.unit AS temperature_unit,
        ca.site_id,
        ca.site_name,
        ca.building_id,
        ca.building_name
    FROM transformed.capabilities_assets ca
    JOIN transformed.telemetry ts ON ca.trend_id = ts.trend_id
    JOIN transformed.dates d ON ts.date_local = d.date
    LEFT JOIN transformed.site_defaults working_hours
        ON (ca.site_id = working_hours.site_id AND working_hours.type = 'WorkingHours')
        AND (working_hours._valid_from <= ts.timestamp_local AND working_hours._valid_to >= ts.timestamp_local)
    WHERE 
        ts.date_local >= '2024-01-01'
    AND model_id IN ('dtmi:com:willowinc:AirTemperatureSensor;1') 
    GROUP BY
        ts.date_local,
        date_hour,
        d.is_weekday,
        ca.unit,
        ca.site_id,
        ca.site_name,
        ca.building_id,
        ca.building_name,
        working_hours.default_value:hourStart,
        working_hours.default_value:hourEnd
    ;

ALTER TASK IF EXISTS transformed.create_table_hourly_temperature_trigger_tk RESUME;