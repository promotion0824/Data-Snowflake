-- ******************************************************************************************************************************
-- DROP custom objects;  NAU now uses the standard sustainability
-- ******************************************************************************************************************************

DROP VIEW IF EXISTS transformed.utility_bills_v;
DROP VIEW IF EXISTS transformed.metering_assets_v;
DROP VIEW IF EXISTS published.ghg_emissions;
DROP VIEW IF EXISTS  published.utility_bills;
DROP PROCEDURE IF EXISTS transformed.create_table_utility_bills_sp();
DROP TABLE IF EXISTS  transformed.utility_bills;
DROP TABLE IF EXISTS  transformed.metering_assets;
DROP TABLE IF EXISTS transformed.utility_bills_raw;
DROP TASK IF EXISTS transformed.create_table_utility_bills_tk;
