-- ******************************************************************************************************************************
-- Create raw ontology tables
-- ******************************************************************************************************************************

CREATE TABLE IF NOT EXISTS raw.stage_tou_nyiso_LBMP (
timestamp					  TIMESTAMP_LTZ,
name						  VARCHAR(255),
ptid						  INTEGER,
lbmp_cost_per_mwhr			  DECIMAL(12,2),
marginal_cost_losses_mwhr 	  DECIMAL(12,2),
marginal_cost_congestion_mwhr DECIMAL(12,2),
file_name 					  VARCHAR(1000),
_ingested_at 				  TIMESTAMP_NTZ DEFAULT SYSDATE()
);


CREATE TABLE IF NOT EXISTS transformed.tou_nyiso_LBMP (
timestamp					  TIMESTAMP_LTZ,
name						  VARCHAR(255),
ptid						  INTEGER,
lbmp_cost_per_mwhr			  DECIMAL(12,2),
marginal_cost_losses_mwhr 	  DECIMAL(12,2),
marginal_cost_congestion_mwhr DECIMAL(12,2),
file_name 					  VARCHAR(1000),
_ingested_at 				  TIMESTAMP_NTZ DEFAULT SYSDATE(),
_last_updated_at 			  TIMESTAMP_NTZ DEFAULT SYSDATE()
);
