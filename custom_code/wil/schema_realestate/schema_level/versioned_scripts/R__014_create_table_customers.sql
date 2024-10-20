-- ------------------------------------------------------------------------------------------------------------------------------
-- Create raw account details table
-- ------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE raw.json_customers (
  _stage_record_id 	VARCHAR(36),
  json_value 		    VARIANT,
  _stage_file_name 	VARCHAR(1000),
  _loader_run_id 		VARCHAR(36),
  _ingested_at 		  TIMESTAMP_NTZ(9),
  _staged_at 			  TIMESTAMP_NTZ(9)
);

CREATE OR REPLACE TABLE  transformed.customers (
    portfolio_id	          VARCHAR(36),
    portfolio_name	        VARCHAR(255),
    portfolio_features_json	VARIANT,
    customer_id	            VARCHAR(36),
    customer_name           VARCHAR(255),
    account_external_id	    VARCHAR(255),
    address1	              VARCHAR(255),
    address2	              VARCHAR(255),
    suburb	                VARCHAR(255),
    post_code	              VARCHAR(20),
    country	                VARCHAR(20),
    status	                VARCHAR(20),
    state	                  VARCHAR(20),
    logo_id	                VARCHAR(36),
    models_of_interest_json	VARIANT,
    customer_features_json	VARIANT,
    model_of_interest_etag	VARIANT,
    sigma_connection_id	    VARCHAR(50),
    _last_updated_at        TIMESTAMP_LTZ
);