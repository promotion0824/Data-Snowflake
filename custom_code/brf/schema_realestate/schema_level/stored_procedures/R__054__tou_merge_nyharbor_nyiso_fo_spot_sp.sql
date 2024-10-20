-- ******************************************************************************************************************************
-- Load toun files
-- ******************************************************************************************************************************
CREATE OR REPLACE PROCEDURE transformed.tou_merge_nyharbor_nyiso_fo_spot_sp()
    RETURNS STRING
    LANGUAGE SQL
	EXECUTE AS CALLER
  AS
    $$
		BEGIN

			MERGE INTO transformed.tou_nyharbor_nyiso_fo_spot AS tgt 
					USING (
						SELECT DISTINCT
							date,
							fo_spot_price,
							file_name,
							_ingested_at
						FROM raw.stage_tou_nyharbor_nyiso_fo_spot
						QUALIFY ROW_NUMBER() OVER (PARTITION BY date ORDER BY _ingested_at DESC) = 1
					) AS src
						ON (tgt.date = src.date)
					WHEN MATCHED THEN
						UPDATE 
						SET
							tgt.fo_spot_price = src.fo_spot_price,
							tgt.file_name = src.file_name,
							tgt._last_updated_at  =  SYSDATE()
					WHEN NOT MATCHED THEN
						INSERT (
							date,
							fo_spot_price,
							file_name,
							_ingested_at,
							_last_updated_at
							)		
						VALUES (
							src.date,
							src.fo_spot_price,
							src.file_name,
							src._ingested_at,
							SYSDATE()
						);
      END;
    $$
;
