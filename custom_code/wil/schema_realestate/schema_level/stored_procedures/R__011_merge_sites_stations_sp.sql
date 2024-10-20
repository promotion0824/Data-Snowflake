-- ******************************************************************************************************************************
-- Stored procedure that merges from raw to transformed
-- USAGE:  CALL transformed.merge_sites_stations_sp();
-- ******************************************************************************************************************************

CREATE OR REPLACE PROCEDURE transformed.merge_sites_stations_sp()
    RETURNS STRING
    LANGUAGE SQL
	EXECUTE AS CALLER
  AS
    $$
		BEGIN
		
		   COPY INTO raw.json_sites_stations (json_value)
		   FROM @raw.ADHOC_ESG/degreedays/site_station.json
		   FILE_FORMAT = (TYPE = 'JSON' strip_outer_array = TRUE);

		   MERGE INTO transformed.sites_stations AS tgt 
		   USING (
			SELECT
				json_value:customer_id::STRING AS customer_id,
				json_value:site_id::STRING AS site_id,
				json_value:site_name::STRING AS site_name,
				json_value:longitude::FLOAT AS longitude,
				json_value:latitude::FLOAT AS latitude,
				json_value:station_id::STRING AS station_id,
				json_value:temperature_unit::STRING AS temperature_unit,
				json_value:temperature_threshold::STRING AS temperature_threshold
			FROM raw.json_sites_stations
			QUALIFY ROW_NUMBER() OVER (PARTITION BY site_id ORDER BY _last_updated_at DESC) = 1
		  ) AS src
			ON (tgt.site_id = src.site_id)
		  WHEN MATCHED THEN
			UPDATE 
			SET 
			  tgt.customer_id = src.customer_id,
			  tgt.site_id = src.site_id,
			  tgt.site_name = src.site_name,
			  tgt.longitude = src.longitude,
			  tgt.latitude = src.latitude,
			  tgt.station_id = src.station_id,
			  tgt.temperature_unit = src.temperature_unit,
			  tgt.temperature_threshold = src.temperature_threshold,
			  tgt._last_updated_at = SYSDATE()
		  WHEN NOT MATCHED THEN
			INSERT (
				customer_id,
				site_id,
				site_name,
				longitude,
				latitude,
				station_id,
				temperature_unit,
				temperature_threshold,
				_last_updated_at
				)		
			VALUES (
				src.customer_id,
				src.site_id, 
				src.site_name,
				src.longitude,
				src.latitude,
				src.station_id,
				src.temperature_unit,
				src.temperature_threshold,
				SYSDATE()
			);
      END;
    $$
;
