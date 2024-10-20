-- ------------------------------------------------------------------------------------------------------------------------------
-- Custom table for historical utility data
-- ------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE transformed.utility_data_historical (
	periodstart DATE,
	periodend DATE,
	consumption NUMBER(38,0),
	unit VARCHAR(16777216),
	consumptioncost NUMBER(38,2)
);