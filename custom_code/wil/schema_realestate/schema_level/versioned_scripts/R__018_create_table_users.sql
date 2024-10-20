

CREATE OR REPLACE TABLE raw.json_users (
  _stage_record_id 	VARCHAR(36),
  json_value 		    VARIANT,
  _stage_file_name 	VARCHAR(1000),
  _loader_run_id 		VARCHAR(36),
  _ingested_at 		  TIMESTAMP_NTZ(9),
  _staged_at 			  TIMESTAMP_NTZ(9)
);

CREATE OR REPLACE STREAM raw.json_users_str
    ON TABLE raw.json_users
    APPEND_ONLY = TRUE
    SHOW_INITIAL_ROWS = TRUE;

CREATE OR REPLACE TABLE transformed.users(
    customer         VARCHAR(50),
    email            VARCHAR(100),
    first_name       VARCHAR(100),
    last_name        VARCHAR(100),
    status           BOOLEAN,
    id               VARCHAR(100),
    group_id         VARCHAR(100),
    group_name       VARCHAR(100),
    group_type_id    VARCHAR(100),
    group_type       VARCHAR(100),
    role_name        VARCHAR(100),
    role_description VARCHAR(100),
    created_date     TIMESTAMP_NTZ,
   	raw_json_value 	 VARIANT,
   	_created_at 		 TIMESTAMP_NTZ DEFAULT SYSDATE(),
   	_last_updated_at TIMESTAMP_NTZ DEFAULT SYSDATE(),
   	_loader_run_id 	 VARCHAR(36),
    _ingested_at 	 TIMESTAMP_NTZ
);