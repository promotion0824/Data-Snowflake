-- ------------------------------------------------------------------------------------------------------------------------------
-- Create file formats
-- Create stage
-- pipe delimiter on csv files is used to get all column names in one field to discover the column names and build dynamically
-- ------------------------------------------------------------------------------------------------------------------------------
USE wil_automation_db;
USE SCHEMA utils;

-- CSV 
CREATE OR REPLACE FILE FORMAT csv_ff 
TYPE = 'CSV'
COMPRESSION = AUTO
field_delimiter = ','
skip_header = 1
FIELD_OPTIONALLY_ENCLOSED_BY='"'
;

CREATE OR REPLACE FILE FORMAT pipe_ff 
TYPE = 'CSV'
COMPRESSION = AUTO
field_delimiter = '|'
;
-- json
CREATE OR REPLACE FILE FORMAT json_ff
TYPE = 'JSON'
STRIP_OUTER_ARRAY = TRUE
;

CREATE OR REPLACE FILE FORMAT parquet_ff
TYPE = 'PARQUET'
COMPRESSION = AUTO
;