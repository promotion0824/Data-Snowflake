-- ******************************************************************************************************************************
-- Create raw ontology tables
-- ******************************************************************************************************************************

CREATE TABLE IF NOT EXISTS raw.stage_tou_rggi_auction (
auction      	    VARCHAR(50),
date	            DATE,
quantity_offered    VARCHAR(50),
ccr_sold            VARCHAR(50),
quantity_sold       VARCHAR(50),
clearing_price      VARCHAR(50),
total_proceeds      VARCHAR(50),
file_name 	        VARCHAR(1000),
_ingested_at        TIMESTAMP_NTZ DEFAULT SYSDATE()
);


CREATE TABLE IF NOT EXISTS transformed.tou_rggi_auction (
auction     	    VARCHAR(50),
date	            DATE,
quantity_offered    INTEGER,
ccr_sold            INTEGER,
quantity_sold       INTEGER,
clearing_price      DECIMAL(18,2),
total_proceeds      DECIMAL(18,2),
file_name 	        VARCHAR(1000),
_ingested_at        TIMESTAMP_NTZ DEFAULT SYSDATE(),
_last_updated_at    TIMESTAMP_NTZ DEFAULT SYSDATE()
);
