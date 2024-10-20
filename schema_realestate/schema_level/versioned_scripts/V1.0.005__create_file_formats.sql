-------------------------------------------------------------------------------------------------------------
-- File formats
-------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FILE FORMAT raw.json_ff
	TYPE = 'JSON'
	COMPRESSION = AUTO
	STRIP_OUTER_ARRAY = TRUE;

CREATE OR REPLACE FILE FORMAT raw.csvgz_ff
	TYPE = 'CSV', 
	FIELD_DELIMITER = ',', 
	FIELD_OPTIONALLY_ENCLOSED_BY='"', 
	COMPRESSION=GZIP;