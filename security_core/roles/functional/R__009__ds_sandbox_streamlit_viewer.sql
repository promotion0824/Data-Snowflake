-- ------------------------------------------------------------------------------------------------------------------------------
-- Create 'ds_sandbox_streamlit_viewer' functional role
-- ------------------------------------------------------------------------------------------------------------------------------

USE ROLE USERADMIN;

{% if hasDsSandboxDb -%}

USE ROLE USERADMIN;
CREATE ROLE IF NOT EXISTS ds_sandbox_streamlit_viewer;

USE ROLE {{ defaultRole }};

GRANT USAGE ON DATABASE ds_sandbox_db TO ROLE ds_sandbox_streamlit_viewer;

GRANT USAGE ON SCHEMA ds_sandbox_db.published TO ROLE ds_sandbox_streamlit_viewer;

GRANT USAGE ON WAREHOUSE streamlit_app_wh TO ROLE ds_sandbox_streamlit_viewer;

GRANT ROLE ds_sandbox_streamlit_viewer TO ROLE SYSADMIN;

GRANT ROLE ds_sandbox_streamlit_viewer TO ROLE data_scientist;

{%- endif %}

USE ROLE {{ defaultRole }};