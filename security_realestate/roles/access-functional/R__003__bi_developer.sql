-- ------------------------------------------------------------------------------------------------------------------------------
-- Grant environment specific access roles to 'bi_developer' functional role
-- ------------------------------------------------------------------------------------------------------------------------------
USE ROLE USERADMIN;

GRANT ROLE {{ environment }}_raw_r TO ROLE bi_developer;
GRANT ROLE {{ environment }}_transformed_r TO ROLE bi_developer;
GRANT ROLE {{ environment }}_published_r TO ROLE bi_developer;
GRANT ROLE {{ environment }}_utils_r TO ROLE bi_developer;

USE ROLE {{ defaultRole }};
