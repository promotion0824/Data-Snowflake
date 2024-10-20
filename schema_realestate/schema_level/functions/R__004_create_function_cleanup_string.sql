-- ------------------------------------------------------------------------------------------------------------------------------
-- Create function
-- removes values that are invalid in column names
-- used for building raw staging table based on csv column names;
-- called from create_table_from_stage stored procedure;
-- ------------------------------------------------------------------------------------------------------------------------------

USE SCHEMA utils;

CREATE OR REPLACE FUNCTION cleanup_string(STR string)
RETURNS string
LANGUAGE javascript AS
$$
    var s = STR;
    s = s.replace(/-/g, "_");             // Replace dash with underscore
    s = s.replace(/\ /g, "_");            // Replace space with underscore
    s = s.replace(/\@/g, "_");            // Replace @ with underscore
    s = s.replace(/\&/g, "_");            // Replace & with underscore	
    s = s.replace(/\./g, "_");            // Replace period with underscore
	s = s.replace(/\(/g, "_");            // Replace ( with underscore
	s = s.replace(/\)/g, "_");            // Replace ) with underscore
    return s;
$$;
