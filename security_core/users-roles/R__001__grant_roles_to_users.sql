-- ------------------------------------------------------------------------------------------------------------------------------
-- Grant roles to users
-- ------------------------------------------------------------------------------------------------------------------------------
USE ROLE USERADMIN;
GRANT ROLE data_engineer TO USER mpampena;
GRANT ROLE data_engineer TO USER tgottwald;
GRANT ROLE data_engineer TO USER sbedwell;
GRANT ROLE data_scientist TO USER pcostello;
GRANT ROLE data_scientist TO USER rbharati;
GRANT ROLE data_scientist TO USER ezhu;
GRANT ROLE data_scientist TO USER dtarekegne;

GRANT ROLE data_scientist TO USER bblack;
GRANT ROLE bi_developer TO USER xwang;
GRANT ROLE bi_developer TO USER lnsantos;
GRANT ROLE analyst TO USER dtarekegne;
GRANT ROLE digital_engineer TO USER mburke;
GRANT ROLE digital_engineer TO USER jtalactac;
GRANT ROLE digital_engineer TO USER ecalzavara;
GRANT ROLE performance_engineer TO USER bblack;
GRANT ROLE performance_engineer TO USER tbendavid;
GRANT ROLE performance_engineer TO USER cmanna;
GRANT ROLE performance_engineer TO USER wroantree;
GRANT ROLE performance_engineer TO USER jturpin;
GRANT ROLE performance_engineer TO USER imercer;
GRANT ROLE performance_engineer TO USER rszcodronski;
GRANT ROLE performance_engineer TO USER igilurrutia;
GRANT ROLE performance_engineer TO USER rbharati;
GRANT ROLE performance_engineer TO USER hneiman;
GRANT ROLE data_engineer TO USER amclachlan;
GRANT ROLE performance_engineer TO USER jgarner;

{% if hasDsSandboxDb -%}
GRANT ROLE bi_tool_ds TO USER bi_tool_ds_usr;
{%- endif %}

{% if hasDsSandboxDb -%}
GRANT ROLE ds_sandbox_streamlit_viewer TO USER csimpson;
GRANT ROLE ds_sandbox_streamlit_viewer TO USER hneiman;
GRANT ROLE ds_sandbox_streamlit_viewer TO USER rszcodronski;
{%- endif %}

{% if hasSustainabilityDb -%}
GRANT ROLE sustainability_loading TO USER sustainability_loading_usr;
{%- endif %}
 
{% if hasDsSandboxDb -%}
GRANT ROLE bi_tool_ds TO USER bi_tool_ds_usr;
{%- endif %}

-- Elevated permissions (SYSADMIN role needed for deployment)
USE ROLE SYSADMIN;
GRANT ROLE SYSADMIN TO USER mpampena;
GRANT ROLE ACCOUNTADMIN TO USER mpampena;
GRANT ROLE SYSADMIN TO USER tgottwald;
GRANT ROLE ACCOUNTADMIN TO USER tgottwald;
GRANT ROLE SYSADMIN TO USER sbedwell;
GRANT ROLE ACCOUNTADMIN TO USER sbedwell;
 
USE ROLE {{ defaultRole }};