-- ******************************************************************************************************************************
-- Stored procedure that merges from raw to transformed
-- USAGE:  CALL transformed.merge_hourly_temperature_sp();
-- NOTE: deployed only to prod because that is the only environment that the weather data gets written to.
-- ******************************************************************************************************************************
	
{% if environment|lower == 'prd' %}

CREATE OR REPLACE PROCEDURE transformed.merge_hourly_temperature_sp()
    RETURNS STRING
    LANGUAGE SQL
	EXECUTE AS CALLER
  AS
    $$
		BEGIN
		
		   COPY INTO raw.json_hourly_temperature
			FROM (
				SELECT $1,SYSDATE() FROM @raw.ADHOC_ESG/degreedays/hourly_weather_data.json
				)
		   FILE_FORMAT = (TYPE = 'JSON' strip_outer_array = TRUE);

		   MERGE INTO transformed.hourly_temperature AS tgt 
		   USING (
			SELECT DISTINCT
				json_value:station_id::string as station_id,
				TO_TIMESTAMP_NTZ(json_value:"0".datetime::varchar) as date_hour,
				json_value:"0".value as temperature,
				ss.temperature_unit
			 FROM raw.json_hourly_temperature ht
			 LEFT JOIN transformed.sites_stations ss
                    ON (ss.station_id = json_value:station_id::string)
             WHERE ht._ingested_at > IFNULL((SELECT max(_created_at) FROM transformed.hourly_temperature),'2022-01-01')
			QUALIFY ROW_NUMBER() OVER (PARTITION BY station_id,date_hour ORDER BY ht._ingested_at DESC) = 1
		  ) AS src
			ON (tgt.station_id = src.station_id) AND (tgt.date_hour = src.date_hour)
		  WHEN MATCHED THEN
			UPDATE 
			SET 
			  tgt.station_id = src.station_id,
			  tgt.date_hour = src.date_hour,
			  tgt.temperature = src.temperature,
			  tgt.temperature_unit = src.temperature_unit,
			  tgt._last_updated_at = SYSDATE()
		  WHEN NOT MATCHED THEN
			INSERT (
				station_id,
				date_hour,
				temperature,
				temperature_unit,
				_last_updated_at
				)		
			VALUES (
				src.station_id,
				src.date_hour, 
				src.temperature,
				src.temperature_unit,
				SYSDATE()
			);
      END;
    $$
;


{% endif %}

SELECT 1;