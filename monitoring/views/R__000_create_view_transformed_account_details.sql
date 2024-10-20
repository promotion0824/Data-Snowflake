-- TODO: Not worth fixing this as we will roll-out new alerting and monitoring

-- -- ------------------------------------------------------------------------------------------------------------------------------
-- -- Create view that provides account details
-- -- ------------------------------------------------------------------------------------------------------------------------------

-- -- Deploy only for customer accounts
-- {% if accountType == 'customer' -%}

-- USE ROLE {{ defaultRole }};
-- -- Create the tables for uat and prd so new deployments will be able to use this view
-- CREATE TABLE IF NOT EXISTS {{ environment }}_db.schemachange.change_history AS SELECT * FROM util_db.schemachange.change_history WHERE 1=0;

-- CREATE OR REPLACE VIEW monitoring_db.transformed.account_details AS 
--     WITH cte_customer AS (

--     SELECT TOP 1 RIGHT(database_name, LEN(database_name) -1) AS identifier
--     FROM snowflake.information_schema.databases
--     WHERE STARTSWITH(database_name, '_')

--     ), cte_prd_schemachange AS (

--     SELECT MAX(version) AS deployment_version, MAX(installed_on) AS last_deployed_at
--     FROM prd_db.schemachange.change_history

--     ), cte_uat_schemachange AS (

--     SELECT MAX(version) AS deployment_version, MAX(installed_on) AS last_deployed_at
--     FROM uat_db.schemachange.change_history
    

--     ), cte_dev_schemachange AS (

--     SELECT MAX(version) AS deployment_version, MAX(installed_on) AS last_deployed_at
--     FROM dev_db.schemachange.change_history
    
--     )
--     SELECT 
--     CURRENT_ACCOUNT() AS account_name,
--     CURRENT_REGION() AS region,
--     identifier AS customer_identifier,
--     CURRENT_VERSION() AS snowflake_version,
--     -- to_object(parse_json('{"prd": {"version": deployment_version }}'))
--     PARSE_JSON('{"prd":{"version":"' || prd.deployment_version || '", "lastDeployedAt":"' || prd.last_deployed_at || '"}, 
--                 "uat":{"version":"' || uat.deployment_version || '", "lastDeployedAt":"' || uat.last_deployed_at || '"},
--                 "dev":{"version":"' || dev.deployment_version || '", "lastDeployedAt":"' || dev.last_deployed_at || '"}
--                 }') AS deployment_details
--     FROM cte_customer
--     CROSS JOIN cte_prd_schemachange prd
--     CROSS JOIN cte_uat_schemachange uat
--     CROSS JOIN cte_dev_schemachange dev
-- ;

-- {%- endif %}

-- USE ROLE {{ defaultRole }};

SELECT 1;