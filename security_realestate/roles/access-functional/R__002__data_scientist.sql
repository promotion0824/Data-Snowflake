-- ------------------------------------------------------------------------------------------------------------------------------
-- Grant environment specific access roles to 'data_scientist' functional role
-- ------------------------------------------------------------------------------------------------------------------------------
USE ROLE USERADMIN;

GRANT ROLE {{ environment }}_raw_r TO ROLE data_scientist;
GRANT ROLE {{ environment }}_transformed_r TO ROLE data_scientist;
GRANT ROLE {{ environment }}_published_r TO ROLE data_scientist;
GRANT ROLE {{ environment }}_utils_r TO ROLE data_scientist;
GRANT ROLE {{ environment }}_ml_r TO ROLE data_scientist;

USE ROLE {{ defaultRole }};