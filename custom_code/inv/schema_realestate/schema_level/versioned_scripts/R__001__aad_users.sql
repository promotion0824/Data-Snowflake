-- ------------------------------------------------------------------------------------------------------------------------------
-- Create AAD users
-- ------------------------------------------------------------------------------------------------------------------------------
USE ROLE USERADMIN;
CREATE USER IF NOT EXISTS mtomasson
  LOGIN_NAME   = 'mtomasson@willowinc.com'
  DISPLAY_NAME = 'Martin'
  FIRST_NAME   = 'Martin'
  LAST_NAME    = 'Tomasson' 
  EMAIL        = 'mtomasson@willowinc.com'
  DEFAULT_ROLE = performance_engineer
  DEFAULT_NAMESPACE = 'prd_db.published'
  DEFAULT_WAREHOUSE = performance_engineer_wh;
GRANT ROLE performance_engineer TO USER mtomasson;
  
USE ROLE {{ defaultRole }};
