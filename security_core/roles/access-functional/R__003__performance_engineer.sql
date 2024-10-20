-- ------------------------------------------------------------------------------------------------------------------------------
-- Grant analytics_db access roles to 'performance_engineer' functional role
-- ------------------------------------------------------------------------------------------------------------------------------
USE ROLE USERADMIN;

{% if hasAnalyticsDb -%}

GRANT ROLE analytics_public_r TO ROLE performance_engineer;
GRANT ROLE analytics_public_w TO ROLE performance_engineer;
GRANT ROLE analytics_public_ddl TO ROLE performance_engineer;

{%- endif %}

{% if hasDsSandboxDb -%}

GRANT ROLE ds_sandbox_published_r TO ROLE performance_engineer;

{%- endif %}

USE ROLE {{ defaultRole }};