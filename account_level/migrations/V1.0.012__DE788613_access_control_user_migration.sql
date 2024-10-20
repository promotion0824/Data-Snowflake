-- ------------------------------------------------------------------------------------
-- Update default roles and warehouses for existing users
-- ------------------------------------------------------------------------------------
USE ROLE USERADMIN;

ALTER USER IF EXISTS mpampena
SET
  DEFAULT_ROLE = data_engineer
  DEFAULT_WAREHOUSE = data_engineer_wh;

ALTER USER IF EXISTS tgottwald
SET
  DEFAULT_ROLE = data_engineer
  DEFAULT_WAREHOUSE = data_engineer_wh;

ALTER USER IF EXISTS pcostello
SET
  DEFAULT_ROLE = data_scientist
  DEFAULT_WAREHOUSE = data_scientist_wh;

ALTER USER IF EXISTS xwang
SET
  DEFAULT_ROLE = bi_developer
  DEFAULT_WAREHOUSE = bi_developer_wh;

ALTER USER IF EXISTS lnsantos
SET
  DEFAULT_ROLE = bi_developer
  DEFAULT_WAREHOUSE = bi_developer_wh;

ALTER USER IF EXISTS dtarekegne
SET
  DEFAULT_ROLE = analyst
  DEFAULT_WAREHOUSE = analyst_wh;

ALTER USER IF EXISTS mburke
SET
  DEFAULT_ROLE = digital_engineer
  DEFAULT_WAREHOUSE = digital_engineer_wh;

ALTER USER IF EXISTS jtalactac
SET
  DEFAULT_ROLE = digital_engineer
  DEFAULT_WAREHOUSE = digital_engineer_wh;

ALTER USER IF EXISTS ecalzavara
SET
  DEFAULT_ROLE = digital_engineer
  DEFAULT_WAREHOUSE = digital_engineer_wh;

ALTER USER IF EXISTS bblack
SET
  DEFAULT_ROLE = performance_engineer
  DEFAULT_WAREHOUSE = performance_engineer_wh;

ALTER USER IF EXISTS tbendavid
SET
  DEFAULT_ROLE = performance_engineer
  DEFAULT_WAREHOUSE = performance_engineer_wh;

ALTER USER IF EXISTS cmanna
SET
  DEFAULT_ROLE = performance_engineer
  DEFAULT_WAREHOUSE = performance_engineer_wh;


ALTER USER IF EXISTS wroantree
SET
  DEFAULT_ROLE = performance_engineer
  DEFAULT_WAREHOUSE = performance_engineer_wh;


ALTER USER IF EXISTS jturpin
SET
  DEFAULT_ROLE = performance_engineer
  DEFAULT_WAREHOUSE = performance_engineer_wh;

ALTER USER IF EXISTS imercer
SET
  DEFAULT_ROLE = performance_engineer
  DEFAULT_WAREHOUSE = performance_engineer_wh;

ALTER USER IF EXISTS rszcodronski
SET
  DEFAULT_ROLE = performance_engineer
  DEFAULT_WAREHOUSE = performance_engineer_wh;

ALTER USER IF EXISTS igilurrutia
SET
  DEFAULT_ROLE = performance_engineer
  DEFAULT_WAREHOUSE = performance_engineer_wh;

ALTER USER IF EXISTS nberk
SET
  DEFAULT_ROLE = performance_engineer
  DEFAULT_WAREHOUSE = performance_engineer_wh;

ALTER USER IF EXISTS sigma_prd_usr
SET
  DEFAULT_ROLE = bi_tool_prd;

ALTER USER IF EXISTS sigma_uat_usr
SET
  DEFAULT_ROLE = bi_tool_uat;

ALTER USER IF EXISTS sigma_dev_usr
SET
  DEFAULT_ROLE = bi_tool_dev;

-- ------------------------------------------------------------------------------------
-- Drop users no longer with the company
-- ------------------------------------------------------------------------------------
DROP USER IF EXISTS kpaix;
DROP USER IF EXISTS dhenleymartin;
DROP USER IF EXISTS mjanos;

USE ROLE {{ defaultRole }};