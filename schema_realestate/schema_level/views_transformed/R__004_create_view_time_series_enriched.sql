-- ********************************************************************************************************************************
-- Create view
-- ********************************************************************************************************************************

CREATE OR REPLACE VIEW transformed.time_series_enriched AS
	SELECT
		ts.date_local,
        ts.timestamp_local,
        ts.timestamp_utc,
		ts.dt_id,
		ts.trend_id,
		ts.site_id,
		ts.telemetry_value,
		LEFT(SPLIT_PART(TIME_SLICE(timestamp_local, 15, 'MINUTE'),' ',2),5) AS time_local_15min,
		LEFT(SPLIT_PART(TIME_SLICE(timestamp_local, 30, 'MINUTE'),' ',2),5) AS time_local_30min,
		LEFT(SPLIT_PART(TIME_SLICE(timestamp_local, 60, 'MINUTE'),' ',2),5) AS time_local_60min,
		TO_TIMESTAMP(CONCAT(TO_CHAR(date_local,'YYYY-MM-DD'),  ' ',  time_local_15min)) AS date_time_local_15min,
		TO_TIMESTAMP(CONCAT(TO_CHAR(date_local,'YYYY-MM-DD'),  ' ',  time_local_60min)) AS date_time_local_hour,
		CAST(CONCAT(SPLIT_PART(TIME_SLICE(timestamp_local, 15, 'MINUTE'),' ',1) , ' ', LEFT(SPLIT_PART(TIME_SLICE(timestamp_local, 60, 'MINUTE'),' ',2),5) ) AS TIMESTAMP_NTZ(0)) AS start_of_hour, --this uses date_local and time_local60
		ts.telemetry_value - LAG(ts.telemetry_value, 1, 0) OVER (PARTITION BY ts.site_id,ts.trend_id ORDER BY ts.site_id, ts.trend_id, ts.date_local, ts.timestamp_local) AS diff_to_prev,
		LAST_VALUE( ts.telemetry_value ) OVER ( PARTITION BY ts.site_id,ts.trend_id, date_local, time_local_15min ORDER BY ts.site_id,ts.trend_id, ts.date_local, ts.timestamp_local ) AS last_value_15min,
		LAST_VALUE( ts.telemetry_value ) OVER ( PARTITION BY ts.site_id,ts.trend_id, date_local, time_local_60min ORDER BY ts.site_id,ts.trend_id, ts.date_local, ts.timestamp_local ) AS last_value_hour,
		ts.enqueued_at,
		ts._last_updated_at
	FROM transformed.telemetry ts
;