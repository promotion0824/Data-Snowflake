-- ------------------------------------------------------------------------------------------------------------------------------
-- Create procedure
-- adds columns to existing table;  
-- called from create_table_from_stage stored procedure; 
-- ------------------------------------------------------------------------------------------------------------------------------

USE SCHEMA utils;

CREATE OR REPLACE PROCEDURE add_column_to_table(tableName VARCHAR, colName VARCHAR, colType VARCHAR)
    RETURNS string
    LANGUAGE JAVASCRIPT
    AS
$$ 
    var sql_command = 'ALTER TABLE ' + TABLENAME + ' ADD COLUMN "' + COLNAME + '" ' + COLTYPE;
    snowflake.execute({sqlText: sql_command});
    return 'success';
$$;
