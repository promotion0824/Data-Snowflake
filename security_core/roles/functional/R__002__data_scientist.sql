-- ------------------------------------------------------------------------------------------------------------------------------
-- Create 'data_scientist' functional role
-- ------------------------------------------------------------------------------------------------------------------------------
USE ROLE USERADMIN;
CREATE ROLE IF NOT EXISTS data_scientist;

USE ROLE {{ defaultRole }};

GRANT USAGE ON WAREHOUSE data_scientist_wh TO ROLE data_scientist;
GRANT OPERATE ON WAREHOUSE data_scientist_wh TO ROLE data_scientist;

GRANT USAGE ON WAREHOUSE data_scientist_snowpark_wh TO ROLE data_scientist;
GRANT OPERATE ON WAREHOUSE data_scientist_snowpark_wh TO ROLE data_scientist;

-- Usage on dummy _<customerAbbrv> database 
GRANT USAGE ON DATABASE _{{ customerAbbrv }} TO ROLE data_scientist;

-- Grant integration_user role
GRANT ROLE integrations_user TO ROLE data_scientist;

GRANT ROLE data_scientist TO ROLE SYSADMIN;

USE ROLE {{ defaultRole }};