-- ******************************************************************************************************************************
-- Create initial tables to support the compliance views
-- The actual tables will be created by the stored procedure create_table_from_stage
-- ******************************************************************************************************************************
USE wil_automation_db;

CREATE OR REPLACE EXTERNAL TABLE ext_transformed_212501_WHT_WesternHarbourTunnel(
  attribute STRING AS (value:Attribute::STRING),
  de_cased STRING AS (value:"DE-CASED"::STRING),
  filename STRING AS (value:FileName::STRING),
  source STRING AS (value:Source::STRING),
  valid STRING AS (value:Valid::STRING),
  data_value STRING AS (value:Value::STRING),
  ifcGUID STRING AS (value:ifcGUID::STRING)
  )
  --partition by (date_part)
  WITH INTEGRATION='EXT_STAGE_ADHOC_DEV_WIL_AUTOMATION_NIN'
  LOCATION=@utils.wil_automation_dev_csv_esg/wil_automation/212501_WHT_WesternHarbourTunnel/A01_Data_Compliance/A01_80_Transform/
  AUTO_REFRESH = true
  FILE_FORMAT = (TYPE = parquet);
 
 -- if external table fails; then create a view that uses infer schema instead:
/*
CREATE OR REPLACE VIEW transformed_212501_WHT_WesternHarbourTunnel AS 
SELECT *
  FROM table(
    infer_schema(
      location=>'@utils.wil_automation_dev_csv_esg/wil_automation/212501_WHT_WesternHarbourTunnel/A01_Data Compliance/A01_80_Transform/_Transformed.parquet'
      , file_format=>'utils.parquet_ff'
      )
    );
*/

CREATE OR REPLACE MATERIALIZED VIEW transformed_212501_WHT_WesternHarbourTunnel AS
	SELECT
		attribute,
		de_cased,
		filename,
		source,
		valid,
		data_value,
		ifcGUID
	FROM ext_transformed_212501_WHT_WesternHarbourTunnel;

