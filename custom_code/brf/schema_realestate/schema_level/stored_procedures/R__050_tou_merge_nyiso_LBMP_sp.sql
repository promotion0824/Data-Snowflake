-- ******************************************************************************************************************************
-- Load toun files
-- ******************************************************************************************************************************

CREATE OR REPLACE PROCEDURE transformed.tou_merge_nyiso_LBMP_sp()
    RETURNS STRING
    LANGUAGE SQL
	EXECUTE AS CALLER
  AS
    $$
		BEGIN
		
			MERGE INTO transformed.tou_nyiso_LBMP AS tgt 
					USING (
						SELECT DISTINCT
							timestamp,
							name,
							ptid,
							lbmp_cost_per_mwhr,
							marginal_cost_losses_mwhr,
							marginal_cost_congestion_mwhr,
							file_name,
							_ingested_at
						FROM raw.stage_tou_nyiso_LBMP
						QUALIFY ROW_NUMBER() OVER (PARTITION BY ptid,timestamp ORDER BY _ingested_at DESC) = 1
					) AS src
						ON (tgt.ptid = src.ptid) AND (tgt.timestamp = src.timestamp)
					WHEN MATCHED THEN
						UPDATE 
						SET
							tgt.name = src.name,
							tgt.lbmp_cost_per_mwhr = src.lbmp_cost_per_mwhr,
							tgt.marginal_cost_losses_mwhr = src.marginal_cost_losses_mwhr,
							tgt.marginal_cost_congestion_mwhr = src.marginal_cost_congestion_mwhr,
							tgt.file_name = src.file_name,
							tgt._last_updated_at  =  SYSDATE()
					WHEN NOT MATCHED THEN
						INSERT (
							timestamp,
							name,
							ptid,
							lbmp_cost_per_mwhr,
							marginal_cost_losses_mwhr,
							marginal_cost_congestion_mwhr,
							file_name,
							_ingested_at,
							_last_updated_at
							)		
						VALUES (
							src.timestamp,
							src.name,
							src.ptid,
							src.lbmp_cost_per_mwhr,
							src.marginal_cost_losses_mwhr,
							src.marginal_cost_congestion_mwhr,
							src.file_name,
							src._ingested_at,
							SYSDATE()
						);
      END;
    $$
;
