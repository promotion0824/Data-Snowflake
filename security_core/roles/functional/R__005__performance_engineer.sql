-- ------------------------------------------------------------------------------------------------------------------------------
-- Create 'performance_engineer' functional role
-- ------------------------------------------------------------------------------------------------------------------------------
USE ROLE USERADMIN;
CREATE ROLE IF NOT EXISTS performance_engineer;

USE ROLE {{ defaultRole }};
GRANT USAGE ON WAREHOUSE performance_engineer_wh TO ROLE performance_engineer;
GRANT OPERATE ON WAREHOUSE performance_engineer_wh TO ROLE performance_engineer;

-- Usage on dummy _<customerAbbrv> database 
GRANT USAGE ON DATABASE _{{ customerAbbrv }} TO ROLE performance_engineer;

GRANT ROLE performance_engineer TO ROLE SYSADMIN;

USE ROLE {{ defaultRole }};