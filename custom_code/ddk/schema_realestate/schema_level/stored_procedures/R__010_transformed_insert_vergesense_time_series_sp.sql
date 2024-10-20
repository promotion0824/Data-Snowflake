-- ******************************************************************************************************************************
-- Stored procedure that incrementally populates the time_series table
-- ******************************************************************************************************************************

CREATE OR REPLACE PROCEDURE transformed.insert_vergesense_time_series_sp()
    RETURNS STRING
    LANGUAGE SQL
  AS
    $$
      BEGIN
        LET watermark TIMESTAMP_NTZ := (SELECT IFNULL(MAX(timestamp_utc),'2022-01-01') AS max_updated FROM transformed.vergesense_time_series);

        INSERT INTO transformed.vergesense_time_series  (
			date_local,
			timestamp_local,
			timestamp_utc,
			trend_id,
			telemetry_value,
			site_id,
			capability_name,
			capability_id,
			start_of_hour,
			analog_value,
			on_count,
			off_count,
			model_id,
			asset_id
			)  
			SELECT
				ts.date_local,
				ts.timestamp_local,
				ts.timestamp_utc,
				LOWER(ts.trend_id) AS trend_id,
				ts.telemetry_value,
				ts.site_id,
				vs.capability_name,
				vs.capability_id,
				ts.start_of_hour,
				Case When model_id = 'dtmi:com:willowinc:PeopleCountSensor;1' Then telemetry_value Else NULL END as analog_value,
				Case When model_id !='dtmi:com:willowinc:PeopleCountSensor;1' AND telemetry_value = 1 Then 1 Else NULL END as on_count,
				Case When model_id !='dtmi:com:willowinc:PeopleCountSensor;1' AND telemetry_value = 0 Then 1 Else NULL END as off_count,
				vs.model_id,
				vs.asset_id
			FROM transformed.time_series_enriched ts
			JOIN transformed.vergesense_assets vs 
			  ON (ts.site_id = vs.site_id)
			 AND (ts.trend_id = vs.trend_id)
			WHERE ts.timestamp_utc > IFNULL(:watermark, TO_TIMESTAMP('0000-01-01'));     
      END;
    $$
;
