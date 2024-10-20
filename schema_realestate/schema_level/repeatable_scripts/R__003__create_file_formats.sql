-- ------------------------------------------------------------------------------------------------------------------------------
-- Create file formats
-- Create stage
-- pipe delimiter on csv files is used to get all column names in one field to discover the column names and build dynamically
-- ------------------------------------------------------------------------------------------------------------------------------

USE SCHEMA utils;

CREATE OR REPLACE FILE FORMAT raw.json_ff
	TYPE = 'JSON'
	COMPRESSION = AUTO
	STRIP_OUTER_ARRAY = TRUE;

CREATE OR REPLACE FILE FORMAT raw.csvgz_ff
	TYPE = 'CSV', 
	FIELD_DELIMITER = ',', 
	FIELD_OPTIONALLY_ENCLOSED_BY='"', 
	COMPRESSION=GZIP;