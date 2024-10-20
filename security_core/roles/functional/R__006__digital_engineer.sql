-- ------------------------------------------------------------------------------------------------------------------------------
-- Create 'digital_engineer' functional role
-- ------------------------------------------------------------------------------------------------------------------------------
USE ROLE USERADMIN;
CREATE ROLE IF NOT EXISTS digital_engineer;

USE ROLE {{ defaultRole }};
GRANT USAGE ON WAREHOUSE digital_engineer_wh TO ROLE digital_engineer;
GRANT OPERATE ON WAREHOUSE digital_engineer_wh TO ROLE digital_engineer;

-- Usage on dummy _<customerAbbrv> database 
GRANT USAGE ON DATABASE _{{ customerAbbrv }} TO ROLE digital_engineer;

GRANT ROLE digital_engineer TO ROLE SYSADMIN;

USE ROLE {{ defaultRole }};