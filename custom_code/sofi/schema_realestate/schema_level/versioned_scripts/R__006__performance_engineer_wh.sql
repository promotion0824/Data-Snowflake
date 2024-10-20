-- ------------------------------------------------------------------------------------------------------------------------------
-- Warehouse used by performance engineers
-- ------------------------------------------------------------------------------------------------------------------------------

USE ROLE {{ defaultRole }};

CREATE WAREHOUSE IF NOT EXISTS  performance_engineer_medium_wh 
  WITH 
    WAREHOUSE_SIZE = 'MEDIUM' 
    AUTO_SUSPEND = 300 
    AUTO_RESUME = TRUE 
    MIN_CLUSTER_COUNT = 1 
    MAX_CLUSTER_COUNT = 1
    SCALING_POLICY = 'ECONOMY' 
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse used by performance engineers.'
;

GRANT OPERATE ON WAREHOUSE performance_engineer_medium_wh TO ROLE sysadmin;
GRANT MONITOR ON WAREHOUSE performance_engineer_medium_wh TO ROLE sysadmin;

GRANT OPERATE ON WAREHOUSE performance_engineer_medium_wh TO ROLE performance_engineer;
GRANT MONITOR ON WAREHOUSE performance_engineer_medium_wh TO ROLE performance_engineer;
