-- ------------------------------------------------------------------------------------------------------------------------------
-- Grant environment specific access roles to 'performance_engineer' functional role
-- ------------------------------------------------------------------------------------------------------------------------------
USE ROLE USERADMIN;

CREATE USER IF NOT EXISTS rbharati
  LOGIN_NAME   = 'rbharati@willowinc.com'
  DISPLAY_NAME = 'Ritika'
  FIRST_NAME   = 'Ritika'
  LAST_NAME    = 'Bhharati' 
  EMAIL        = 'rbharati@willowinc.com'
;

GRANT ROLE data_scientist TO USER rbharati;

USE ROLE {{ defaultRole }};