-- ******************************************************************************************************************************
-- Stored procedure that merges from raw to transformed
-- USAGE:  CALL transformed.merge_weather_data_sp();
-- ****************************************************************************************************************************** 
CREATE OR REPLACE PROCEDURE transformed.merge_weather_data_sp()
    RETURNS STRING
    LANGUAGE SQL
	EXECUTE AS CALLER
  AS
    $$
		BEGIN
		
		   COPY INTO raw.json_weather_data(json_value)
		   FROM @raw.ADHOC_ESG/degreedays/weather_data.json
		   FILE_FORMAT = (TYPE = 'JSON' strip_outer_array = TRUE);

		   MERGE INTO transformed.weather_data AS tgt 
		   USING (
			SELECT DISTINCT
				 json_value:col_station_id::string AS station_id,
				 TO_TIMESTAMP((json_value:col_0CDD.firstDay/1000)::integer) AS date,
				 json_value:col_0CDD.value AS cdd,
				 json_value:col_0HDD.value AS hdd
			 FROM raw.json_weather_data
			 WHERE station_id IS NOT NULL
			QUALIFY ROW_NUMBER() OVER (PARTITION BY station_id,date ORDER BY _last_updated_at DESC) = 1
		  ) AS src
			ON (tgt.station_id = src.station_id) AND (tgt.date = src.date)
		  WHEN MATCHED THEN
			UPDATE 
			SET 
			  tgt.station_id = src.station_id,
			  tgt.date = src.date,
			  tgt.cdd = src.cdd,
			  tgt.hdd = src.hdd,
			  tgt._last_updated_at = SYSDATE()
		  WHEN NOT MATCHED THEN
			INSERT (
				station_id,
				date,
				cdd,
				hdd,
				_last_updated_at
				)		
			VALUES (
				src.station_id,
				src.date, 
				src.cdd,
				src.hdd,
				SYSDATE()
			);
      END;
    $$
;
