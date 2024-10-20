-- ------------------------------------------------------------------------------------------------------------------------------
-- Create 'analyst' functional role
-- ------------------------------------------------------------------------------------------------------------------------------
USE ROLE USERADMIN;
CREATE ROLE IF NOT EXISTS analyst;

USE ROLE {{ defaultRole }};
GRANT USAGE ON WAREHOUSE analyst_wh TO ROLE analyst;
GRANT OPERATE ON WAREHOUSE analyst_wh TO ROLE analyst;

-- Usage on dummy _<customerAbbrv> database 
GRANT USAGE ON DATABASE _{{ customerAbbrv }} TO ROLE analyst;
GRANT ROLE analyst TO ROLE SYSADMIN;

USE ROLE {{ defaultRole }};