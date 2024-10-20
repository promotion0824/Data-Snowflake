USE ROLE deployment_pipeline;
USE WAREHOUSE deployment_pipeline_wh;

DECLARE
    row_count NUMBER;
    wh_row_count NUMBER;
    role_row_count NUMBER;
BEGIN
    SHOW WAREHOUSES LIKE 'ml_pipeline_dev_wh';

    SELECT COUNT(*) INTO wh_row_count
    FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

    SHOW ROLES LIKE 'ml_pipeline_dev';

    SELECT COUNT(*) INTO role_row_count
    FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

    IF (wh_row_count > 0 AND role_row_count > 0) THEN
        GRANT USAGE ON WAREHOUSE ml_pipeline_dev_wh TO ROLE ml_pipeline_dev;
        GRANT OPERATE ON WAREHOUSE ml_pipeline_dev_wh TO ROLE ml_pipeline_dev;
    END IF;

    SHOW USERS LIKE 'ml_pipeline_dev_usr';

    SELECT COUNT(*) INTO row_count
    FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

    IF (row_count > 0) THEN
        ALTER USER ml_pipeline_dev_usr SET DEFAULT_WAREHOUSE = ml_pipeline_dev_wh;
    END IF;
    
END;

DECLARE
    row_count NUMBER;
    wh_row_count NUMBER;
    role_row_count NUMBER;
BEGIN
    SHOW WAREHOUSES LIKE 'ml_pipeline_uat_wh';

    SELECT COUNT(*) INTO wh_row_count
    FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

    SHOW ROLES LIKE 'ml_pipeline_uat';

    SELECT COUNT(*) INTO role_row_count
    FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

    IF (wh_row_count > 0 AND role_row_count > 0) THEN
        GRANT USAGE ON WAREHOUSE ml_pipeline_uat_wh TO ROLE ml_pipeline_uat;
        GRANT OPERATE ON WAREHOUSE ml_pipeline_uat_wh TO ROLE ml_pipeline_uat;
    END IF;

    SHOW USERS LIKE 'ml_pipeline_uat_usr';

    SELECT COUNT(*) INTO row_count
    FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

    IF (row_count > 0) THEN
        ALTER USER ml_pipeline_uat_usr SET DEFAULT_WAREHOUSE = ml_pipeline_uat_wh;
    END IF;
    
END;


DECLARE
    row_count NUMBER;
    wh_row_count NUMBER;
    role_row_count NUMBER;
BEGIN
    SHOW WAREHOUSES LIKE 'ml_pipeline_prd_wh';

    SELECT COUNT(*) INTO wh_row_count
    FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

    SHOW ROLES LIKE 'ml_pipeline_prd';

    SELECT COUNT(*) INTO role_row_count
    FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

    IF (wh_row_count > 0 AND role_row_count > 0) THEN
        GRANT USAGE ON WAREHOUSE ml_pipeline_prd_wh TO ROLE ml_pipeline_prd;
        GRANT OPERATE ON WAREHOUSE ml_pipeline_prd_wh TO ROLE ml_pipeline_prd;
    END IF;

    SHOW USERS LIKE 'ml_pipeline_prd_usr';

    SELECT COUNT(*) INTO row_count
    FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

    IF (row_count > 0) THEN
        ALTER USER ml_pipeline_prd_usr SET DEFAULT_WAREHOUSE = ml_pipeline_prd_wh;
    END IF;
    
END;

DECLARE
    row_count NUMBER;
BEGIN
    SHOW WAREHOUSES LIKE 'data_pipeline_dev_wh';

    SELECT COUNT(*) INTO row_count
    FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

    IF (row_count > 0) THEN
        GRANT USAGE ON WAREHOUSE data_pipeline_dev_wh TO ROLE data_pipeline_prd;
        GRANT OPERATE ON WAREHOUSE data_pipeline_dev_wh TO ROLE data_pipeline_prd;
    END IF;

    SHOW USERS LIKE 'data_pipeline_dev_usr';

    SELECT COUNT(*) INTO row_count
    FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

    IF (row_count > 0) THEN
        USE ROLE SECURITYADMIN;
        ALTER USER data_pipeline_dev_usr SET DEFAULT_WAREHOUSE = data_pipeline_dev_wh;
        USE ROLE deployment_pipeline;
    END IF;
    
END;


DECLARE
    row_count NUMBER;
BEGIN
    SHOW WAREHOUSES LIKE 'data_pipeline_uat_wh';

    SELECT COUNT(*) INTO row_count
    FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

    IF (row_count > 0) THEN
        GRANT USAGE ON WAREHOUSE data_pipeline_uat_wh TO ROLE data_pipeline_prd;
        GRANT OPERATE ON WAREHOUSE data_pipeline_uat_wh TO ROLE data_pipeline_prd;
    END IF;

    SHOW USERS LIKE 'data_pipeline_uat_usr';

    SELECT COUNT(*) INTO row_count
    FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

    IF (row_count > 0) THEN
        USE ROLE SECURITYADMIN;
        ALTER USER data_pipeline_uat_usr SET DEFAULT_WAREHOUSE = data_pipeline_uat_wh;
        USE ROLE deployment_pipeline;
    END IF;
    
END;


DECLARE
    row_count NUMBER;
BEGIN
    SHOW WAREHOUSES LIKE 'data_pipeline_prd_wh';

    SELECT COUNT(*) INTO row_count
    FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

    IF (row_count > 0) THEN
        GRANT USAGE ON WAREHOUSE data_pipeline_prd_wh TO ROLE data_pipeline_prd;
        GRANT OPERATE ON WAREHOUSE data_pipeline_prd_wh TO ROLE data_pipeline_prd;
    END IF;

    SHOW USERS LIKE 'data_pipeline_prd_usr';

    SELECT COUNT(*) INTO row_count
    FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

    IF (row_count > 0) THEN
        USE ROLE SYSADMIN;
        ALTER USER data_pipeline_prd_usr SET DEFAULT_WAREHOUSE = data_pipeline_prd_wh;
        USE ROLE deployment_pipeline;
    END IF;
    
END;

DECLARE
    row_count NUMBER;
    wh_row_count NUMBER;
    role_row_count NUMBER;
BEGIN
    SHOW WAREHOUSES LIKE 'sigma_dev_wh';

    SELECT COUNT(*) INTO wh_row_count
    FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

    SHOW ROLES LIKE 'bi_tool_dev';

    SELECT COUNT(*) INTO role_row_count
    FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

    IF (wh_row_count > 0 AND role_row_count > 0) THEN
        GRANT USAGE ON WAREHOUSE sigma_dev_wh TO ROLE bi_tool_dev;
        GRANT OPERATE ON WAREHOUSE sigma_dev_wh TO ROLE bi_tool_dev;
    END IF;

    SHOW USERS LIKE 'sigma_dev_usr';

    SELECT COUNT(*) INTO row_count
    FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

    IF (row_count > 0) THEN
        USE ROLE SECURITYADMIN;
        ALTER USER sigma_dev_usr SET DEFAULT_WAREHOUSE = sigma_dev_wh;
        USE ROLE deployment_pipeline;
    END IF;
END;

DECLARE
    row_count NUMBER;
    wh_row_count NUMBER;
    role_row_count NUMBER;
BEGIN
    SHOW WAREHOUSES LIKE 'sigma_uat_wh';

    SELECT COUNT(*) INTO wh_row_count
    FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

    SHOW ROLES LIKE 'bi_tool_uat';

    SELECT COUNT(*) INTO role_row_count
    FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

    IF (wh_row_count > 0 AND role_row_count > 0) THEN
        GRANT USAGE ON WAREHOUSE sigma_uat_wh TO ROLE bi_tool_uat;
        GRANT OPERATE ON WAREHOUSE sigma_uat_wh TO ROLE bi_tool_uat;
    END IF;

    SHOW USERS LIKE 'sigma_uat_usr';

    SELECT COUNT(*) INTO row_count
    FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

    IF (row_count > 0) THEN
        USE ROLE SECURITYADMIN;
        ALTER USER sigma_uat_usr SET DEFAULT_WAREHOUSE = sigma_uat_wh;
        USE ROLE deployment_pipeline;
    END IF;
END;

DECLARE
    row_count NUMBER;
    wh_row_count NUMBER;
    role_row_count NUMBER;
BEGIN
    SHOW WAREHOUSES LIKE 'sigma_prd_wh';

    SELECT COUNT(*) INTO wh_row_count
    FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

    SHOW ROLES LIKE 'bi_tool_prd';

    SELECT COUNT(*) INTO role_row_count
    FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

    IF (wh_row_count > 0 AND role_row_count > 0) THEN
        GRANT USAGE ON WAREHOUSE sigma_prd_wh TO ROLE bi_tool_prd;
        GRANT OPERATE ON WAREHOUSE sigma_prd_wh TO ROLE bi_tool_prd;
    END IF;

    SHOW USERS LIKE 'sigma_prd_usr';

    SELECT COUNT(*) INTO row_count
    FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

    IF (row_count > 0) THEN
        USE ROLE SECURITYADMIN;
        ALTER USER sigma_prd_usr SET DEFAULT_WAREHOUSE = sigma_prd_wh;
        USE ROLE deployment_pipeline;
    END IF;
END;


