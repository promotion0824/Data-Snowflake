-- ------------------------------------------------------------------------------------------------------------------------------
-- Create users for reporting tools
-- ------------------------------------------------------------------------------------------------------------------------------

USE ROLE SECURITYADMIN;

-- Power BI
CREATE ROLE IF NOT EXISTS bitool_powerbi;  

GRANT USAGE ON DATABASE prd_db TO ROLE bitool_powerbi;
GRANT USAGE ON SCHEMA prd_db.published TO ROLE bitool_powerbi;
GRANT USAGE ON WAREHOUSE bitool_powerbi_wh TO ROLE bitool_powerbi;
GRANT OPERATE ON WAREHOUSE bitool_powerbi_wh TO ROLE bitool_powerbi;
GRANT SELECT ON ALL VIEWS IN SCHEMA prd_db.published TO ROLE bitool_powerbi;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA prd_db.published TO ROLE bitool_powerbi;

CREATE USER IF NOT EXISTS biuser_powerbi
  LOGIN_NAME   = 'biuser_powerbi'
  DEFAULT_ROLE = bitool_powerbi
  DEFAULT_WAREHOUSE = bitool_powerbi_wh
  PASSWORD = '';   -- deploy manually with real password (stored in lastpass and key vault); 

GRANT ROLE bitool_powerbi TO USER biuser_powerbi;

