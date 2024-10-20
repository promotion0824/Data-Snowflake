-- ******************************************************************************************************************************
-- Stored procedure to populate table used by dashboard pivot table for asset hierarchy
-- ******************************************************************************************************************************

CREATE OR REPLACE PROCEDURE transformed.transformed_create_table_electrical_metering_hierarchy_sp()
    RETURNS STRING
    LANGUAGE SQL
    EXECUTE AS CALLER
  AS
    $$
		CREATE OR REPLACE TABLE transformed.electrical_metering_hierarchy AS 
		SELECT * 
		FROM transformed.electrical_metering_assets_v;
		;
    $$
;