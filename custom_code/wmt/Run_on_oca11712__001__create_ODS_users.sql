-- ------------------------------------------------------------------------------------------------------------------------------
-- Create main <environment>_db
-- ------------------------------------------------------------------------------------------------------------------------------

USE ROLE ACCOUNTADMIN;

CREATE USER IF NOT EXISTS mpampena
  PASSWORD     = ''
  MUST_CHANGE_PASSWORD = true
  LOGIN_NAME   = 'mpampena@willowinc.com'
  DISPLAY_NAME = 'Matt'
  FIRST_NAME   = 'Matt'
  LAST_NAME    = 'Pampena' 
  EMAIL        = 'mpampena@willowinc.com'
  DEFAULT_ROLE =  WMT_READER_ODS
  DEFAULT_NAMESPACE = 'willow_db.published'
  DEFAULT_WAREHOUSE = walmart_ods_wh;
GRANT ROLE WMT_READER TO USER mpampena;
GRANT ROLE WMT_READER_ODS TO USER mpampena;
CREATE USER IF NOT EXISTS kwattula
  PASSWORD     = ''
  MUST_CHANGE_PASSWORD = true
  LOGIN_NAME   = 'kwattula@willowinc.com'
  DISPLAY_NAME = 'Kyle'
  FIRST_NAME   = 'Kyle'
  LAST_NAME    = 'Wattula' 
  EMAIL        = 'kwattula@willowinc.com'
  DEFAULT_ROLE =  WMT_READER_ODS
  DEFAULT_NAMESPACE = 'willow_db.published'
  DEFAULT_WAREHOUSE = walmart_ods_wh;
GRANT ROLE WMT_READER_ODS TO USER kwattula;

CREATE USER IF NOT EXISTS cmanna
  PASSWORD     = 'changetheeffinpasswordNOW2'
  MUST_CHANGE_PASSWORD = true
  LOGIN_NAME   = 'cmanna@willowinc.com'
  DISPLAY_NAME = 'Chris'
  FIRST_NAME   = 'Chris'
  LAST_NAME    = 'Manna' 
  EMAIL        = 'cmanna@willowinc.com'
  DEFAULT_ROLE =  WMT_READER_ODS
  DEFAULT_NAMESPACE = 'willow_db.published'
  DEFAULT_WAREHOUSE = walmart_ods_wh;
GRANT ROLE WMT_READER_ODS TO USER cmanna;

CREATE USER IF NOT EXISTS bkrovvidi
  PASSWORD     = ''
  MUST_CHANGE_PASSWORD = true
  LOGIN_NAME   = 'Bharathkumar.Krovvidi@walmart.com'
  DISPLAY_NAME = 'Bharathkumar'
  FIRST_NAME   = 'Bharathkumar'
  LAST_NAME    = 'Krovvidi' 
  EMAIL        = 'Bharathkumar.Krovvidi@walmart.com'
  DEFAULT_ROLE =  WMT_READER_ODS
  DEFAULT_NAMESPACE = 'willow_db.published'
  DEFAULT_WAREHOUSE = walmart_ods_wh;
GRANT ROLE WMT_READER_ODS TO USER bkrovvidi;

CREATE USER IF NOT EXISTS vgali
  PASSWORD     = ''
  MUST_CHANGE_PASSWORD = true
  LOGIN_NAME   = 'Venkatasivakumar.Gali@walmart.com'
  DISPLAY_NAME = 'Venkatasivakumar'
  FIRST_NAME   = 'Venkatasivakumar'
  LAST_NAME    = 'Gali' 
  EMAIL        = 'Venkatasivakumar.Gali@walmart.com'
  DEFAULT_ROLE =  WMT_READER_ODS
  DEFAULT_NAMESPACE = 'willow_db.published'
  DEFAULT_WAREHOUSE = walmart_ods_wh;
GRANT ROLE WMT_READER_ODS TO USER vgali;

CREATE USER IF NOT EXISTS amohan
  PASSWORD     = ''
  MUST_CHANGE_PASSWORD = true
  LOGIN_NAME   = 'Anoop.mohan@walmart.com'
  DISPLAY_NAME = 'Anoop'
  FIRST_NAME   = 'Anoop'
  LAST_NAME    = 'Mohan' 
  EMAIL        = 'Anoop.mohan@walmart.com'
  DEFAULT_ROLE =  WMT_READER_ODS
  DEFAULT_NAMESPACE = 'willow_db.published'
  DEFAULT_WAREHOUSE = walmart_ods_wh;
GRANT ROLE WMT_READER_ODS TO USER amohan;

CREATE USER IF NOT EXISTS abhandarkar
  PASSWORD     = ''
  MUST_CHANGE_PASSWORD = true
  LOGIN_NAME   = 'anant.bhandarkar@walmart.com'
  DISPLAY_NAME = 'Anant'
  FIRST_NAME   = 'Anant'
  LAST_NAME    = 'Bhandarkar' 
  EMAIL        = 'anant.bhandarkar@walmart.com'
  DEFAULT_ROLE =  WMT_READER_ODS
  DEFAULT_NAMESPACE = 'willow_db.published'
  DEFAULT_WAREHOUSE = walmart_ods_wh;
GRANT ROLE WMT_READER_ODS TO USER abhandarkar;

CREATE USER IF NOT EXISTS akhamitkar
  PASSWORD     = ''
  MUST_CHANGE_PASSWORD = true
  LOGIN_NAME   = 'Adarsh.Khamitkar@walmart.com'
  DISPLAY_NAME = 'Adarsh'
  FIRST_NAME   = 'Adarsh'
  LAST_NAME    = 'Khamitkar' 
  EMAIL        = 'Adarsh.Khamitkar@walmart.com'
  DEFAULT_ROLE =  WMT_READER_ODS
  DEFAULT_NAMESPACE = 'willow_db.published'
  DEFAULT_WAREHOUSE = walmart_ods_wh;
GRANT ROLE WMT_READER_ODS TO USER akhamitkar;
