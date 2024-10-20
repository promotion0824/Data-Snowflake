-- ******************************************************************************************************************************
-- Load toun files
-- ******************************************************************************************************************************
CREATE OR REPLACE PROCEDURE transformed.tou_merge_nyiso_load_forecast_sp()
    RETURNS STRING
    LANGUAGE SQL
	EXECUTE AS CALLER
  AS
    $$
		BEGIN
		
			MERGE INTO transformed.tou_nyiso_load_forecast AS tgt 
					USING (
						SELECT DISTINCT
							time_stamp,
							capitl,centrl,
							dunwod,genese,
							hud_vl,
							longil,
							mhk_vl,
							millwd,
							nyc,
							north,
							west,
							nyiso,
							file_name,
							_ingested_at
						FROM raw.stage_tou_nyiso_load_forecast
						QUALIFY ROW_NUMBER() OVER (PARTITION BY time_stamp ORDER BY _ingested_at DESC) = 1
					) AS src
						ON (tgt.time_stamp = src.time_stamp)
					WHEN MATCHED THEN
						UPDATE 
						SET
							tgt.time_stamp = src.time_stamp,
							tgt.capitl = src.capitl,
							tgt.centrl = src.centrl,
							tgt.dunwod = src.dunwod,
							tgt.genese = src.genese,
							tgt.hud_vl = src.hud_vl,
							tgt.longil = src.longil,
							tgt.mhk_vl = src.mhk_vl,
							tgt.millwd = src.millwd,
							tgt.nyc = src.nyc,
							tgt.north = src.north,
							tgt.west = src.west,
							tgt.nyiso = src.nyiso,
							tgt.file_name = src.file_name,
							tgt._ingested_at = src._ingested_at,
							tgt._last_updated_at  =  SYSDATE()
					WHEN NOT MATCHED THEN
						INSERT (
							time_stamp,
							capitl,
							centrl,
							dunwod,
							genese,
							hud_vl,
							longil,
							mhk_vl,
							millwd,
							nyc,
							north,
							west,
							nyiso,
							file_name,
							_ingested_at,
							_last_updated_at
							)		
						VALUES (
							src.time_stamp,
							src.capitl,
							src.centrl,
							src.dunwod,
							src.genese,
							src.hud_vl,
							src.longil,
							src.mhk_vl,
							src.millwd,
							src.nyc,
							src.north,
							src.west,
							src.nyiso,
							src.file_name,
							src._ingested_at,
							SYSDATE()
						);
      END;
    $$
;
