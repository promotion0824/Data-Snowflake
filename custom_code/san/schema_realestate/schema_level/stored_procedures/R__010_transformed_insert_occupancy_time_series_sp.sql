-- ******************************************************************************************************************************
-- Stored procedure that incrementally populates the time_series table
-- ******************************************************************************************************************************

CREATE OR REPLACE PROCEDURE transformed.insert_occupancy_time_series_sp()
    RETURNS STRING
    LANGUAGE SQL
  AS
    $$
      BEGIN
        LET watermark TIMESTAMP_NTZ := (SELECT IFNULL(MAX(timestamp_local),'2022-01-01') AS max_updated FROM transformed.occupancy_time_series);

        INSERT INTO transformed.occupancy_time_series  (
			date_local,
			date_time_local_hour,
			date_time_local_15min,
			timestamp_local,
			enqueued_at_utc,
			trend_id,
			telemetry_value,
			last_value_hour,
			site_id,
			capability_name,
			capability_id,
			start_of_hour,
			model_id,
			asset_id
			)  
			SELECT
				ts.date_local,
				ts.date_time_local_hour,
				ts.date_time_local_15min,
				ts.timestamp_local,
				ts.enqueued_at AS enqueued_at_utc,
				LOWER(ts.trend_id) AS trend_id,
				ts.telemetry_value,
				LAST_VALUE(ts.telemetry_value ) OVER ( PARTITION BY ts.trend_id, ts.date_time_local_hour ORDER BY ts.timestamp_local) AS last_value_hour,
				ts.site_id,
				vs.capability_name,
				vs.capability_id,
				ts.start_of_hour,
				vs.model_id,
				vs.asset_id
			FROM transformed.time_series_enriched ts
			JOIN transformed.occupancy_assets vs 
			  ON (ts.site_id = vs.site_id)
			 AND (ts.trend_id = vs.trend_id)
			WHERE ts.timestamp_local > IFNULL(:watermark, TO_TIMESTAMP('0000-01-01'));     
      END;
    $$
;
