-- ******************************************************************************************************************************
-- Create view
-- in Transformed folder because other transformed views use it
-- ******************************************************************************************************************************

CREATE OR REPLACE VIEW transformed.telemetry_stream_enriched AS
	SELECT
		ts.date_local,
        ts.timestamp_local,
        ts.timestamp_utc,
		ts.dt_id,
		ts.trend_id,
		ts.site_id,
		ts.telemetry_value,
		LEFT(SPLIT_PART(TIME_SLICE(timestamp_local, 15, 'MINUTE'),' ',2),5) AS time_local_15min,
		TO_TIMESTAMP(CONCAT(TO_CHAR(date_local,'YYYY-MM-DD'),  ' ',  time_local_15min)) AS date_time_local_15min,
		DATE_TRUNC(HOUR, date_time_local_15min) AS date_time_local_hour,
		ts.enqueued_at AS enqueued_at_utc
	FROM transformed.telemetry_str ts

UNION ALL 
-- Need to include the previous hour too;
	SELECT
		ts.date_local,
        ts.timestamp_local,
        ts.timestamp_utc,
		ts.dt_id,
		ts.trend_id,
		ts.site_id,
		ts.telemetry_value,
		ts.time_local_15min,
		ts.date_time_local_15min,
		ts.date_time_local_hour,
		ts.enqueued_at AS enqueued_at_utc
	FROM transformed.time_series_enriched ts
	WHERE ts.timestamp_local > dateadd('hour', -1, (SELECT MIN(timestamp_local) FROM transformed.telemetry_str))
	  AND ts.timestamp_local < (SELECT MIN(timestamp_local) FROM transformed.telemetry_str)
;
