-- ------------------------------------------------------------------------------------------------------------------------------
-- Create 'sustainability_loading' functional role
-- ------------------------------------------------------------------------------------------------------------------------------
{% if hasSustainabilityDb -%}

USE ROLE USERADMIN;
CREATE ROLE IF NOT EXISTS sustainability_loading;

USE ROLE {{ defaultRole }};

-- Grant access role to sustainability_db
GRANT ROLE sustainability_published_r TO ROLE sustainability_loading;
GRANT ROLE sustainability_loading TO ROLE SYSADMIN;

-- Grant usage and operate on warehouse
GRANT USAGE ON WAREHOUSE sustainability_loading_wh TO ROLE sustainability_loading;
GRANT OPERATE ON WAREHOUSE sustainability_loading_wh TO ROLE sustainability_loading;
{%- endif %}

USE ROLE {{ defaultRole }};