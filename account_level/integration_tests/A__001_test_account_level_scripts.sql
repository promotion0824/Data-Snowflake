-- ******************************************************************************************************************************
-- Tests for Account level deployment verification
-- These all need to be reworked
-- ******************************************************************************************************************************

-- R__002_create_databases.sql:
-- SET row_count = (SELECT COUNT(*) FROM information_schema.databases WHERE database_name ILIKE '{{ environment }}_db');
-- SELECT '{{ environment }}_db count:' AS obj_name, $row_count, 1/$row_count AS result;

-- -- R__003_create_schemas.sql:
-- SET row_count = (SELECT COUNT(*) FROM {{ environment }}_db.information_schema.schemata WHERE catalog_name ILIKE '{{ environment }}_db');
-- SELECT '{{ environment }}_db.schema count:' AS obj_name, $row_count, 1/CASE WHEN $row_count < 4 THEN 0 ELSE $row_count END AS result;

-- R__004__create_integrations.sql

SHOW INTEGRATIONS;
SET row_count = (SELECT count(*) FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
				 WHERE "name" IN (
								'EXT_TELEMETRY_STAGE_{{ uppercaseEnvironment }}_SIN',
								'EXT_STAGE_{{ uppercaseEnvironment }}_SIN',
								'EXT_STAGE_ADHOC_{{ uppercaseEnvironment }}_SIN',
								'EXT_STAGE_{{ uppercaseEnvironment }}_NIN',
								'EXT_TELEMETRY_STAGE_{{ uppercaseEnvironment }}_NIN'
								 )
				);
-- SELECT 'dev Integrations count:' AS obj_name, $row_count, 1/CASE WHEN $row_count < 5 THEN 0 ELSE $row_count END AS result;


-- R__005__create_warehouses.sql
SHOW WAREHOUSES;
-- SET row_count = (SELECT COUNT(*) FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
-- 				 WHERE "name" IN (
-- 								'{{ uppercaseEnvironment }}_WH',
-- 								'{{ uppercaseEnvironment }}_ELT_WH'
-- 								 )
-- 				);
-- SELECT 'dev Warehouse count:' AS obj_name, $row_count, 1/CASE WHEN $row_count != 2 THEN 0 ELSE $row_count END AS result;


-- R__006__create_users_roles.sql
-- Ensure sysadmin can execute tasks
SHOW GRANTS TO ROLE TASK_ADMIN;
-- SET row_count = (SELECT COUNT(*) FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
--                  WHERE "privilege" IN ('EXECUTE MANAGED TASK', 'EXECUTE TASK') AND "grantee_name" = 'TASK_ADMIN'
--                 );
-- SELECT 'Task permissions count:' AS obj_name, $row_count, 1/CASE WHEN $row_count != 2 THEN 0 ELSE $row_count END AS result;
  
-- Ensure BI_DEVELOPER role has appropriate permissions      
-- 2023-08-07 commented out because new access conrol permissions seemed to have changed this.    
--SHOW GRANTS TO role BI_DEVELOPER;

-- SET row_count = (SELECT COUNT(*) FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
-- 				 WHERE ("privilege" = 'USAGE' AND "granted_on" = 'ROLE' AND "name" LIKE 'ANALYST')
-- 				    OR ("privilege" = 'USAGE' AND "granted_on" = 'SCHEMA' AND "name" LIKE '%PUBLISHED%')
-- 				    OR ("granted_on" = 'WAREHOUSE' AND "name" iLIKE 'dev_wh' AND "privilege" IN ('USAGE','OPERATE'))
-- 				);
-- SELECT 'BI_DEVELOPER permissions count:' AS obj_name, $row_count, 1/CASE WHEN $row_count < 4 THEN 0 ELSE $row_count END AS result;

-- Ensure ANALYST role has appropriate permissions 
-- 2023-08-07 commented out because new access conrol permissions seemed to have changed this.
SHOW GRANTS TO role ANALYST;
-- SET row_count = (SELECT COUNT(*) FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
-- 				 WHERE ("privilege" = 'MODIFY' AND "name" = 'ANALYTICS_DB.PUBLIC'));
-- SELECT 'ANALYST permissions count:' AS obj_name, $row_count, 1/CASE WHEN $row_count < 1 THEN 0 ELSE $row_count END AS result;
-- adding this statement again because schema change does not allow ending a script with a comment.
SHOW GRANTS TO role ANALYST;