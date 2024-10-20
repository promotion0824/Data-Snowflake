-- ******************************************************************************************************************************
-- Load toun files
-- ******************************************************************************************************************************
CREATE OR REPLACE PROCEDURE transformed.tou_merge_rggi_auction_sp()
    RETURNS STRING
    LANGUAGE SQL
	EXECUTE AS CALLER
  AS
    $$
		BEGIN

			MERGE INTO transformed.tou_rggi_auction AS tgt 
					USING (
						SELECT DISTINCT
							auction,
							date,
							TO_NUMBER(REPLACE(quantity_offered,'--')) AS quantity_offered,
							TO_NUMBER(NULLIF((REPLACE(ccr_sold,'--','')),'')) AS ccr_sold,
							TO_NUMBER(REPLACE(quantity_sold,'--','')) AS quantity_sold,
							TO_DECIMAL(NULLIF(REPLACE(clearing_price,'$'),'--'),18,2) AS clearing_price,
							TO_DECIMAL(NULLIF(REPLACE(REPLACE(total_proceeds,'$',''),','),'--'),18,2) AS total_proceeds,
							file_name,
							_ingested_at
						FROM raw.stage_tou_rggi_auction
						QUALIFY ROW_NUMBER() OVER (PARTITION BY auction,date ORDER BY _ingested_at DESC) = 1
					) AS src
						ON (tgt.auction = src.auction) AND (tgt.date = src.date)
					WHEN MATCHED THEN
						UPDATE 
						SET
							tgt.auction = src.auction,
							tgt.date = src.date,
							tgt.quantity_offered = src.quantity_offered,
							tgt.ccr_sold = src.ccr_sold,
							tgt.quantity_sold = src.quantity_sold,
							tgt.clearing_price = src.clearing_price,
							tgt.total_proceeds = src.total_proceeds,
							tgt.file_name = src.file_name,
							tgt._last_updated_at  =  SYSDATE()
					WHEN NOT MATCHED THEN
						INSERT (
							auction,
							date,
							quantity_offered,
							ccr_sold,
							quantity_sold,
							clearing_price,
							total_proceeds,
							file_name,
							_ingested_at,
							_last_updated_at
							)		
						VALUES (
							src.auction,
							src.date,
							src.quantity_offered,
							src.ccr_sold,
							src.quantity_sold,
							src.clearing_price,
							src.total_proceeds,
							src.file_name,
							src._ingested_at,
							SYSDATE()
						);
      END;
    $$
;
