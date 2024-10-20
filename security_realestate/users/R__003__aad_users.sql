-- ------------------------------------------------------------------------------------------------------------------------------
-- Create AAD users

-- The base users are created at account setup - from Data-Core_Snowflake;
-- ------------------------------------------------------------------------------------------------------------------------------
USE ROLE USERADMIN;

CREATE USER IF NOT EXISTS mburke
  LOGIN_NAME   = 'mburke@willowinc.com'
  DISPLAY_NAME = 'Michael'
  FIRST_NAME   = 'Michael'
  LAST_NAME    = 'Burke' 
  EMAIL        = 'mburke@willowinc.com'
  DEFAULT_ROLE = digital_engineer
  DEFAULT_NAMESPACE = 'prd_db.published'
  DEFAULT_WAREHOUSE = digital_engineer_wh;
GRANT ROLE digital_engineer TO USER mburke;

CREATE USER IF NOT EXISTS ecalzavara
  LOGIN_NAME   = 'ecalzavara@willowinc.com'
  DISPLAY_NAME = 'Enrico'
  FIRST_NAME   = 'Enrico'
  LAST_NAME    = 'Calzavara' 
  EMAIL        = 'ecalzavara@willowinc.com'
  DEFAULT_ROLE = digital_engineer
  DEFAULT_NAMESPACE = 'prd_db.published'
  DEFAULT_WAREHOUSE = digital_engineer_wh;

CREATE USER IF NOT EXISTS bblack
  LOGIN_NAME   = 'bblack@willowinc.com'
  DISPLAY_NAME = 'Brandon'
  FIRST_NAME   = 'Brandon'
  LAST_NAME    = 'Black' 
  EMAIL        = 'bblack@willowinc.com'
  DEFAULT_ROLE = performance_engineer
  DEFAULT_NAMESPACE = 'uat_db.published'
  DEFAULT_WAREHOUSE = performance_engineer_wh;

CREATE USER IF NOT EXISTS tbendavid
  LOGIN_NAME   = 'tbendavid@willowinc.com'
  DISPLAY_NAME = 'Tom'
  FIRST_NAME   = 'Tom'
  LAST_NAME    = 'Ben-David' 
  EMAIL        = 'tbendavid@willowinc.com'
  DEFAULT_ROLE = performance_engineer
  DEFAULT_NAMESPACE = 'uat_db.published'
  DEFAULT_WAREHOUSE = performance_engineer_wh;

CREATE USER IF NOT EXISTS cmanna
  LOGIN_NAME   = 'cmanna@willowinc.com'
  DISPLAY_NAME = 'Chris'
  FIRST_NAME   = 'Chris'
  LAST_NAME    = 'Manna' 
  EMAIL        = 'cmanna@willowinc.com'
  DEFAULT_ROLE = performance_engineer
  DEFAULT_NAMESPACE = 'uat_db.published'
  DEFAULT_WAREHOUSE = performance_engineer_wh;

CREATE USER IF NOT EXISTS wroantree
  LOGIN_NAME   = 'wroantree@willowinc.com'
  DISPLAY_NAME = 'Will'
  FIRST_NAME   = 'Will'
  LAST_NAME    = 'Roantree' 
  EMAIL        = 'wroantree@willowinc.com'
  DEFAULT_ROLE = performance_engineer
  DEFAULT_NAMESPACE = 'uat_db.published'
  DEFAULT_WAREHOUSE = performance_engineer_wh;

CREATE USER IF NOT EXISTS imercer
  LOGIN_NAME   = 'imercer@willowinc.com'
  DISPLAY_NAME = 'Ian'
  FIRST_NAME   = 'Ian'
  LAST_NAME    = 'Mercer' 
  EMAIL        = 'imercer@willowinc.com'
  DEFAULT_ROLE = performance_engineer
  DEFAULT_NAMESPACE = 'uat_db.published'
  DEFAULT_WAREHOUSE = performance_engineer_wh;

CREATE USER IF NOT EXISTS rszcodronski
  LOGIN_NAME   = 'rszcodronski@willowinc.com'
  DISPLAY_NAME = 'Rick'
  FIRST_NAME   = 'Rick'
  LAST_NAME    = 'Szcodronski' 
  EMAIL        = 'rszcodronski@willowinc.com'
  DEFAULT_ROLE = performance_engineer
  DEFAULT_NAMESPACE = 'uat_db.published'
  DEFAULT_WAREHOUSE = performance_engineer_wh;

CREATE USER IF NOT EXISTS igilurrutia
  LOGIN_NAME   = 'igilurrutia@willowinc.com'
  DISPLAY_NAME = 'Ignacio'
  FIRST_NAME   = 'Ignacio'
  LAST_NAME    = 'Gil Urrutia' 
  EMAIL        = 'igilurrutia@willowinc.com'
  DEFAULT_ROLE = performance_engineer
  DEFAULT_NAMESPACE = 'uat_db.published'
  DEFAULT_WAREHOUSE = performance_engineer_wh;

CREATE USER IF NOT EXISTS nberk
  LOGIN_NAME   = 'nberk@willowinc.com'
  DISPLAY_NAME = 'Nathaniel'
  FIRST_NAME   = 'Nathaniel'
  LAST_NAME    = 'Berk' 
  EMAIL        = 'nberk@willowinc.com'
  DEFAULT_ROLE = performance_engineer
  DEFAULT_NAMESPACE = 'uat_db.published'
  DEFAULT_WAREHOUSE = performance_engineer_wh;

CREATE USER IF NOT EXISTS rbharati
  LOGIN_NAME   = 'rbharati@willowinc.com'
  DISPLAY_NAME = 'Ritika'
  FIRST_NAME   = 'Ritika'
  LAST_NAME    = 'Bhharati' 
  EMAIL        = 'rbharati@willowinc.com'
  DEFAULT_ROLE = performance_engineer
  DEFAULT_NAMESPACE = 'prd_db.published'
  DEFAULT_WAREHOUSE = performance_engineer_wh;

  CREATE USER IF NOT EXISTS fwhitmore
  LOGIN_NAME   = 'fwhitmore@willowinc.com'
  DISPLAY_NAME = 'Forrest'
  FIRST_NAME   = 'Forrest'
  LAST_NAME    = 'Whitmore' 
  EMAIL        = 'fwhitmore@willowinc.com'
  DEFAULT_ROLE = performance_engineer
  DEFAULT_NAMESPACE = 'uat_db.published'
  DEFAULT_WAREHOUSE = performance_engineer_wh;

  CREATE USER IF NOT EXISTS jbass
  LOGIN_NAME   = 'jbass@willowinc.com'
  DISPLAY_NAME = 'Jameson'
  FIRST_NAME   = 'Jameson'
  LAST_NAME    = 'Bass' 
  EMAIL        = 'jbass@willowinc.com'
  DEFAULT_ROLE = performance_engineer
  DEFAULT_NAMESPACE = 'uat_db.published'
  DEFAULT_WAREHOUSE = performance_engineer_wh;

  CREATE USER IF NOT EXISTS plenz
  LOGIN_NAME   = 'plenz@willowinc.com'
  DISPLAY_NAME = 'Payton'
  FIRST_NAME   = 'Payton'
  LAST_NAME    = 'Lenz' 
  EMAIL        = 'plenz@willowinc.com'
  DEFAULT_ROLE = performance_engineer
  DEFAULT_NAMESPACE = 'prd_db.published'
  DEFAULT_WAREHOUSE = performance_engineer_wh;
GRANT ROLE performance_engineer TO USER plenz;

  CREATE USER IF NOT EXISTS cneipling
  LOGIN_NAME   = 'cneipling@willowinc.com'
  DISPLAY_NAME = 'Chad'
  FIRST_NAME   = 'Chad'
  LAST_NAME    = 'Neipling' 
  EMAIL        = 'cneipling@willowinc.com'
  DEFAULT_ROLE = performance_engineer
  DEFAULT_NAMESPACE = 'prd_db.published'
  DEFAULT_WAREHOUSE = performance_engineer_wh;
GRANT ROLE performance_engineer TO USER cneipling;

CREATE USER IF NOT EXISTS jtidd
  LOGIN_NAME   = 'jtidd@willowinc.com'
  DISPLAY_NAME = 'Jon'
  FIRST_NAME   = 'Jon'
  LAST_NAME    = 'Tidd' 
  EMAIL        = 'jtidd@willowinc.com'
  DEFAULT_ROLE = performance_engineer
  DEFAULT_NAMESPACE = 'prd_db.published'
  DEFAULT_WAREHOUSE = performance_engineer_wh;

GRANT ROLE performance_engineer TO USER jtidd;

USE ROLE {{ defaultRole }};
