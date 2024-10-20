-- ------------------------------------------------------------------------------------------------------------------------------
-- Grant Data Science Sandbox DB published reader access role to 'bi_tool_ds' functional role
-- ------------------------------------------------------------------------------------------------------------------------------

USE ROLE USERADMIN;

{% if hasDsSandboxDb -%}

GRANT ROLE ds_sandbox_published_r TO ROLE bi_tool_ds;

{%- endif %}

USE ROLE {{ defaultRole }};