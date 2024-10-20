CREATE OR REPLACE PROCEDURE utils.load_from_stage_data_compliance_sp()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS '
	
	var stmt = snowflake.createStatement({
	// remove any rows that are not header rows.
    sqlText: `DELETE FROM data_compliance.adhoc_stage_pipe_csv WHERE column_headers NOT ILIKE ''Filename%''`
    });
    stmt.execute();

	var stmt = snowflake.createStatement({
	// insert to new table - this clears from stream
    sqlText: `CREATE OR REPLACE TEMPORARY TABLE data_compliance.temp_adhoc_stage AS SELECT * FROM data_compliance.adhoc_stage_pipe_csv;`
    });
    stmt.execute();

	// get list of columns in csv file so we can build staging table;
	var sqlCmd =`SELECT DISTINCT 
				split_part(_stage_file_name,''/'',2) AS customer,
			    split_part(_stage_file_name,''/'',3) AS project_type,
			    split_part(_stage_file_name,''/'',4) AS file_type
			   FROM data_compliance.temp_adhoc_stage
			   WHERE _stage_file_name ilike ''wil_automation%parquet'';`;
	var stmt = snowflake.createStatement( {sqlText: sqlCmd} );
    var header_result = stmt.execute();
	// loop through each customer and file_type
	var customer = '''';
    var project_type = '''';
	var file_type = '''';  
	//validate or transform file?
    while(header_result.next()) {
        customer 	 = header_result.getColumnValue(1);
		project_type = header_result.getColumnValue(2);
		file_type 	 = header_result.getColumnValue(3);

		var file_type_array = [];
		var table_name = "data_compliance.ext_" + file_type + "_" + customer;
		table_name = table_name.replace(/\\s/g, '''');    		// replace spaces in table name
		var stage_name = "''@utils.wil_automation_dev_csv_esg/wil_automation/" + customer + "/" + project_type + "/" + file_type + "/''"
		
		var sqlCmd = "CREATE EXTERNAL TABLE " + table_name + "(
					  attribute STRING AS (value:Attribute::STRING),
					  de_cased STRING AS (value:"DE-CASED"::STRING),
					  filename STRING AS (value:FileName::STRING),
					  source STRING AS (value:Source::STRING),
					  valid STRING AS (value:Valid::STRING),
					  data_value STRING AS (value:Value::STRING),
					  ifcGUID STRING AS (value:ifcGUID::STRING)
					  )
					  WITH INTEGRATION=''EXT_STAGE_ADHOC_DEV_WIL_AUTOMATION_NIN''
					  LOCATION=" + stage_name + " AUTO_REFRESH = true FILE_FORMAT = (TYPE = parquet)";
		var stmt = snowflake.createStatement( {sqlText: sqlCmd} );
		var rows_result = stmt.execute(); 

		// create materialize view;
		var sqlCmd = "CREATE OR REPLACE MATERIALIZED VIEW " + file_type + "_" + customer + "AS
	SELECT
		attribute,
		de_cased,
		filename,
		source,
		valid,
		data_value,
		ifcGUID
	FROM data_compliance.ext_" + file_type + "_" + customer;
		var stmt = snowflake.createStatement( {sqlText: sqlCmd, binds: [table_name]} );
		var rows_result = stmt.execute(); 
		}
		
    return ''success'';
	
  ';