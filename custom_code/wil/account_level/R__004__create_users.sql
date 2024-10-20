-- ------------------------------------------------------------------------------------------------------------------------------
-- Create users
-- ------------------------------------------------------------------------------------------------------------------------------

USE ROLE SECURITYADMIN;

CREATE USER IF NOT EXISTS segment_loading_usr
  LOGIN_NAME   = 'segment_loading_usr'
  DEFAULT_ROLE = segment_loading
  DEFAULT_WAREHOUSE = segment_loading_wh
  PASSWORD = '';   -- deploy manually with real password (stored in lastpass and key vault); 

GRANT ROLE segment_loading TO USER segment_loading_usr;

--------------------------------------------------------------------------------------------
CREATE USER IF NOT EXISTS inv_engagement_sigma_usr
  LOGIN_NAME   = 'inv_engagement_sigma_usr'
  DEFAULT_ROLE = segment_reporting
  DEFAULT_WAREHOUSE = segment_reporting_wh
  DEFAULT_NAMESPACE = segment_reporting.published
  PASSWORD = '';   -- deploy manually with real password (stored in lastpass and key vault); 

GRANT ROLE segment_reporting TO USER inv_engagement_sigma_usr;

--------------------------------------------------------------------------------------------
CREATE USER IF NOT EXISTS weather_loading_usr
  LOGIN_NAME   = 'weather_loading_usr'
  DEFAULT_ROLE = weather_loading
  DEFAULT_WAREHOUSE = weather_loading_wh
  DEFAULT_NAMESPACE = uat_db.published
  PASSWORD = '';   -- deploy manually with real password (stored in lastpass and key vault); 

GRANT ROLE weather_loading TO USER weather_loading_usr;


-------------------------------------------------------------------------------------------
USE ROLE {{ defaultRole }};