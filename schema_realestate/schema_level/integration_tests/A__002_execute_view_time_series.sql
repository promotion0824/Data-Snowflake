-- ******************************************************************************************************************************
-- SELECT Top 1 from time_series view(s)
-- add a date filter for quicker execution
-- ******************************************************************************************************************************

SELECT TOP 10 
	timestamp_utc,
	telemetry_value,
	timestamp_local,
	date_local,
	time_local_15min,
	time_local_30min,
	time_local_60min,
	date_time_local_15min,
	date_time_local_hour,
	start_of_hour
FROM transformed.time_series_enriched WHERE timestamp_utc >= DATEADD('d',-1,current_timestamp);

SELECT TOP 1 * FROM published.hvac_occupancy_15minute WHERE date_local >= DATEADD('d',-1,current_timestamp);

SELECT TOP 1 * FROM published.occupancy_15minute WHERE date_local >= DATEADD('d',-1,current_timestamp);
