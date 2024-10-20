CREATE USER IF NOT EXISTS egardiner
  LOGIN_NAME   = 'egardiner@willowinc.com'
  DISPLAY_NAME = 'EJ'
  FIRST_NAME   = 'EJ'
  LAST_NAME    = 'Gardiner' 
  EMAIL        = 'egardiner@willowinc.com'
  DEFAULT_ROLE = digital_engineer
  DEFAULT_NAMESPACE = 'prd_db.published'
  DEFAULT_WAREHOUSE = digital_engineer_wh;
GRANT ROLE digital_engineer TO USER egardiner;

