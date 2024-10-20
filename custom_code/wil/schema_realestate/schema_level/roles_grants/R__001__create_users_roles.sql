-- ------------------------------------------------------------------------------------------------------------------------------
-- Create Users and roles
-- ------------------------------------------------------------------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;
CREATE ROLE IF NOT EXISTS digital_engineering;

USE ROLE SECURITYADMIN;

CREATE USER IF NOT EXISTS mburke
  LOGIN_NAME   = 'mburke@willowinc.com'
  DISPLAY_NAME = 'Michael'
  FIRST_NAME   = 'Michael'
  LAST_NAME    = 'Burke' 
  EMAIL        = 'mburke@willowinc.com'
  DEFAULT_ROLE = digital_engineering
  DEFAULT_WAREHOUSE = wil_automation_wh;
GRANT ROLE digital_engineering TO USER mburke;

CREATE USER IF NOT EXISTS ecalzavara
  LOGIN_NAME   = 'ecalzavara@willowinc.com'
  DISPLAY_NAME = 'Enrico'
  FIRST_NAME   = 'Enrico'
  LAST_NAME    = 'Calzavara' 
  EMAIL        = 'ecalzavara@willowinc.com'
  DEFAULT_ROLE = digital_engineering
  DEFAULT_WAREHOUSE = wil_automation_wh;
GRANT ROLE digital_engineering TO USER ecalzavara;
CREATE USER IF NOT EXISTS akasa
  LOGIN_NAME   = 'akasa@willowinc.com'
  DISPLAY_NAME = 'Andi'
  FIRST_NAME   = 'Andi'
  LAST_NAME    = 'Kasa' 
  EMAIL        = 'akasa@willowinc.com'
  DEFAULT_ROLE = digital_engineering
  DEFAULT_WAREHOUSE = wil_automation_wh;
GRANT ROLE digital_engineering TO USER akasa;

CREATE USER IF NOT EXISTS mpunch
  LOGIN_NAME   = 'mpunch@willowinc.com'
  DISPLAY_NAME = 'Mitchell'
  FIRST_NAME   = 'Mitchell'
  LAST_NAME    = 'Punch' 
  EMAIL        = 'mpunch@willowinc.com'
  DEFAULT_ROLE = digital_engineering
  DEFAULT_WAREHOUSE = wil_automation_wh;
GRANT ROLE digital_engineering TO USER mpunch;

USE ROLE SECURITYADMIN;
GRANT USAGE ON DATABASE wil_automation_db TO ROLE digital_engineering;
GRANT ALL ON SCHEMA wil_automation_db.data_compliance TO ROLE digital_engineering;
GRANT ALL ON SCHEMA wil_automation_db.utils TO ROLE digital_engineering;
GRANT SELECT ON ALL TABLES IN schema wil_automation_db.data_compliance TO ROLE digital_engineering;
GRANT SELECT ON ALL views IN schema wil_automation_db.data_compliance TO ROLE digital_engineering;

USE ROLE ACCOUNTADMIN;
CREATE USER IF NOT EXISTS BIUser_digital_engineering PASSWORD ='user106' default_role = digital_engineering must_change_password = true default_warehouse = wil_automation_wh;


GRANT ROLE digital_engineering TO USER BIUser_digital_engineering;

ALTER USER BIUser_digital_engineering SET DEFAULT_WAREHOUSE = 'wil_automation_wh';
ALTER USER BIUser_digital_engineering SET DEFAULT_NAMESPACE = 'wil_automation_db';

GRANT ALL ON future tables IN schema wil_automation_db.data_compliance TO ROLE digital_engineering;
GRANT ALL ON future views  IN schema wil_automation_db.data_compliance TO ROLE digital_engineering;
GRANT ALL ON future tables IN schema wil_automation_db.utils TO ROLE digital_engineering;
GRANT ALL ON future views  IN schema wil_automation_db.utils TO ROLE digital_engineering;

-- This needs TO be run first TO enable serverless tasks
USE ROLE ACCOUNTADMIN;
GRANT EXECUTE MANAGED TASK ON ACCOUNT TO ROLE SYSADMIN;
GRANT EXECUTE TASK ON ACCOUNT TO ROLE SYSADMIN;
GRANT IMPORTED PRIVILEGES ON database snowflake TO role sysadmin;

USE ROLE {{ defaultRole }};

GRANT USAGE ON warehouse wil_automation_wh TO role digital_engineering;
GRANT OPERATE ON warehouse wil_automation_wh TO role digital_engineering;

GRANT USAGE ON warehouse wil_automation_wh TO role digital_engineering;
GRANT OPERATE ON warehouse wil_automation_wh TO role digital_engineering;

GRANT USAGE ON warehouse wil_automation_wh TO USER powerbi_wil_automation;
GRANT OPERATE ON warehouse wil_automation_wh TO USER powerbi_wil_automation;


-- #########################################################################
GRANT USAGE ON DATABASE central_monitoring_db TO ROLE digital_engineering;
GRANT USAGE ON SCHEMA central_monitoring_db.published TO ROLE digital_engineering;
GRANT SELECT ON VIEW central_monitoring_db.published.embedded_widgets TO ROLE digital_engineering;

GRANT ROLE digital_engineering TO ROLE sysadmin;
