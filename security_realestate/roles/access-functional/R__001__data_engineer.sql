-- ------------------------------------------------------------------------------------------------------------------------------
-- Grant environment specific access roles to 'data_engineer' functional role
-- ------------------------------------------------------------------------------------------------------------------------------
USE ROLE USERADMIN;

GRANT ROLE {{ environment }}_raw_r TO ROLE data_engineer;
GRANT ROLE {{ environment }}_transformed_r TO ROLE data_engineer;
GRANT ROLE {{ environment }}_published_r TO ROLE data_engineer;
GRANT ROLE {{ environment }}_utils_r TO ROLE data_engineer;
GRANT ROLE {{ environment }}_schemachange_r TO ROLE data_engineer;

GRANT ROLE {{ environment }}_ml_r TO ROLE data_engineer;

-- DDL/Write permissions for non-prd environments only
{% if environment != 'prd' -%}

GRANT ROLE {{ environment }}_raw_w TO ROLE data_engineer;
GRANT ROLE {{ environment }}_raw_ddl TO ROLE data_engineer;
GRANT ROLE {{ environment }}_transformed_w TO ROLE data_engineer;
GRANT ROLE {{ environment }}_transformed_ddl TO ROLE data_engineer;
GRANT ROLE {{ environment }}_published_ddl TO ROLE data_engineer;

GRANT ROLE {{ environment }}_utils_w TO ROLE data_engineer;
GRANT ROLE {{ environment }}_utils_ddl TO ROLE data_engineer;
GRANT ROLE {{ environment }}_schemachange_w TO ROLE data_engineer;
GRANT ROLE {{ environment }}_schemachange_ddl TO ROLE data_engineer;
GRANT ROLE {{ environment }}_ml_w TO ROLE data_engineer;

GRANT ROLE {{ environment }}_ml_ddl TO ROLE data_engineer;

{%- endif %}

USE ROLE {{ defaultRole }};
