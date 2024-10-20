-- ------------------------------------------------------------------------------------------------------------------------------
-- Create AAD users
-- ------------------------------------------------------------------------------------------------------------------------------
USE ROLE USERADMIN;

CREATE USER IF NOT EXISTS amagee
  LOGIN_NAME   = 'amagee@willowinc.com'
  DISPLAY_NAME = 'Andrew'
  FIRST_NAME   = 'Andrew'
  LAST_NAME    = 'Magee' 
  EMAIL        = 'amagee@willowinc.com'
  DEFAULT_ROLE = performance_engineer
  DEFAULT_NAMESPACE = 'prd_db.published'
  DEFAULT_WAREHOUSE = performance_engineer_wh;
GRANT ROLE performance_engineer TO USER amagee;

CREATE USER IF NOT EXISTS ndomselaar
  LOGIN_NAME   = 'ndomselaar@willowinc.com'
  DISPLAY_NAME = 'Neil'
  FIRST_NAME   = 'Neil'
  LAST_NAME    = 'Domselaar' 
  EMAIL        = 'ndomselaar@willowinc.com'
  DEFAULT_ROLE = performance_engineer
  DEFAULT_NAMESPACE = 'prd_db.published'
  DEFAULT_WAREHOUSE = performance_engineer_wh;

GRANT ROLE performance_engineer TO USER ndomselaar;

CREATE USER IF NOT EXISTS shallows
  LOGIN_NAME   = 'shallows@willowinc.com'
  DISPLAY_NAME = 'Stuart'
  FIRST_NAME   = 'Stuart'
  LAST_NAME    = 'Hallows' 
  EMAIL        = 'shallows@willowinc.com'
  DEFAULT_ROLE = performance_engineer
  DEFAULT_NAMESPACE = 'prd_db.published'
  DEFAULT_WAREHOUSE = performance_engineer_wh;
  
GRANT ROLE performance_engineer TO USER shallows;

CREATE USER IF NOT EXISTS xzhang
  LOGIN_NAME   = 'xzhang@willowinc.com'
  DISPLAY_NAME = 'Xufeng'
  FIRST_NAME   = 'Xufeng'
  LAST_NAME    = 'Zhang' 
  EMAIL        = 'xzhang@willowinc.com'
  DEFAULT_ROLE = performance_engineer
  DEFAULT_NAMESPACE = 'prd_db.published'
  DEFAULT_WAREHOUSE = performance_engineer_wh;

GRANT ROLE performance_engineer TO USER xzhang;

CREATE USER IF NOT EXISTS yzhou
  LOGIN_NAME   = 'yzhou@willowinc.com'
  DISPLAY_NAME = 'Ying'
  FIRST_NAME   = 'Ying'
  LAST_NAME    = 'Zhou' 
  EMAIL        = 'yzhou@willowinc.com'
  DEFAULT_ROLE = performance_engineer
  DEFAULT_NAMESPACE = 'prd_db.published'
  DEFAULT_WAREHOUSE = performance_engineer_wh;

GRANT ROLE performance_engineer TO USER yzhou;
USE ROLE {{ defaultRole }};
