-- ******************************************************************************************************************************
-- Create procedure to import any csv file; discovering the column names and creating the table to load to
/*
-- USAGE:
	call utils.create_table_from_stage('@wil_automation_csv_esg/Compliance/Validation', 'data_compliance.raw_validation');
	select * from data_compliance.raw_validation;

	call utils.create_table_from_stage('@wil_automation_csv_esg/Test', 'raw_test');
	select * from raw_test;
*/
-- ******************************************************************************************************************************

USE SCHEMA utils;

CREATE OR REPLACE PROCEDURE create_table_from_stage(stageFile VARCHAR, tableName VARCHAR)
RETURNS string
LANGUAGE JAVASCRIPT
AS
$$
    var sqlCmd = "CREATE OR REPLACE TABLE " + TABLENAME + " (_stage_file_name VARCHAR(2000), _file_row_number INTEGER, _ingested_at TIMESTAMP_NTZ(9) default current_timestamp());";
    var stmt = snowflake.createStatement( {sqlText: sqlCmd, binds: [TABLENAME]} );
    var rows_result = stmt.execute(); 

    // get list of columns in csv file; parameter values must be upper-case
    var sqlColsList = "SELECT utils.CLEANUP_STRING(value) AS column_name FROM (SELECT $1 AS columnlist FROM " + STAGEFILE +  " (FILE_FORMAT => 'pipe_ff', PATTERN=>'.*(csv|csv.gz)$') t LIMIT 1)," 
    sqlColsList = sqlColsList + "LATERAL SPLIT_TO_TABLE(columnlist, ',') ORDER BY index";
    var stmt = snowflake.createStatement( {sqlText: sqlColsList} );
    var rows_result = stmt.execute(); 
    var cols_array = [];
    var rawColList = '';

    // loop through list; add each column;  (we cannot build the whole statement and execute it at once because variables are limited in size.)
    while(rows_result.next()) {
        cols_array.push( rows_result.getColumnValue(1) );
    }
    for (var i = 0; i < cols_array.length; i++) {
		if (cols_array[i] > '') {
			rawColList = rawColList + '$' + (i+1).toString().toLowerCase() + ',';
			var sqlCmd = "CALL utils.add_column_to_table(?,?,'VARCHAR(2000)')";
			var stmt = snowflake.createStatement( {sqlText: sqlCmd, binds: [TABLENAME, cols_array[i]]} );
			var rows_result = stmt.execute();
			}
	}
    // remove trailing comma;
    rawColList = rawColList.slice(0, -1);
    // load table from file;
    var sqlCmd = "COPY  INTO " + TABLENAME + " FROM (SELECT metadata$filename, metadata$file_row_number, current_timestamp, " + rawColList + " FROM " + STAGEFILE + " (FILE_FORMAT => 'utils.csv_ff', PATTERN=>'.*(csv|csv.gz)$'))";
    var stmt = snowflake.createStatement( {sqlText: sqlCmd, binds: [TABLENAME]} );
    var rows_result = stmt.execute(); 
    return 'success';
$$;
