-- ------------------------------------------------------------------------------------------------------------------------------
-- Create procedure
-- adds columns to existing table;  
-- called from create_table_from_stage stored procedure; 
-- (was required because a single variable does not have enough capacity to handle the full dynamic create table statement)
-- ------------------------------------------------------------------------------------------------------------------------------
USE wil_automation_db;
USE SCHEMA utils;

CREATE OR REPLACE PROCEDURE add_column_to_table(tableName VARCHAR, colName VARCHAR, colType VARCHAR)
    RETURNS string
    LANGUAGE JAVASCRIPT
    AS
$$ 
    var sql_command = "ALTER TABLE " + TABLENAME + " ADD COLUMN " + COLNAME + " " + COLTYPE;
    snowflake.execute({sqlText: sql_command});
    return 'success';
$$;

GRANT USAGE ON PROCEDURE add_column_to_table(VARCHAR, VARCHAR, VARCHAR) TO role digital_engineering;
