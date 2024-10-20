-- ******************************************************************************************************************************
-- Load toun files
-- ******************************************************************************************************************************
CREATE OR REPLACE PROCEDURE transformed.merge_tou_hhng_spot_sp()
    RETURNS STRING
    LANGUAGE SQL
	EXECUTE AS CALLER
  AS
    $$
		BEGIN
		
			COPY INTO raw.stage_tou_hhng_spot
				FROM  (SELECT $2,$3,metadata$filename,SYSDATE() FROM @raw.ADHOC_ESG/toun/hhng_spot_files/hhng_spot_files.csv)
				FILE_FORMAT = (TYPE='CSV' SKIP_HEADER=3  COMPRESSION='NONE' TIMESTAMP_FORMAT='MM/DD/YYYY HH24:MI')
				;

			MERGE INTO transformed.tou_hhng_spot AS tgt 
					USING (
						SELECT DISTINCT
							date,
							ng_spot_price,
							file_name,
							_ingested_at
						FROM raw.stage_tou_hhng_spot
						QUALIFY ROW_NUMBER() OVER (PARTITION BY date ORDER BY _ingested_at DESC) = 1
					) AS src
						ON (tgt.date = src.date)
					WHEN MATCHED THEN
						UPDATE 
						SET
							tgt.ng_spot_price = src.ng_spot_price,
							tgt.file_name = src.file_name,
							tgt._last_updated_at  =  SYSDATE()
					WHEN NOT MATCHED THEN
						INSERT (
							date,
							ng_spot_price,
							file_name,
							_ingested_at,
							_last_updated_at
							)		
						VALUES (
							src.date,
							src.ng_spot_price,
							src.file_name,
							src._ingested_at,
							SYSDATE()
						);
      END;
    $$
;
