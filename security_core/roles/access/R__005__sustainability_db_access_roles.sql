-- ------------------------------------------------------------------------------------------------------------------------------
-- Create sustainability_db access roles
-- ------------------------------------------------------------------------------------------------------------------------------
{% if hasSustainabilityDb -%}

USE ROLE USERADMIN;
CREATE ROLE IF NOT EXISTS sustainability_published_r;
USE ROLE {{ defaultRole }};

GRANT USAGE ON DATABASE sustainability_db TO ROLE sustainability_published_r;
GRANT USAGE ON SCHEMA sustainability_db.published TO ROLE sustainability_published_r;
GRANT SELECT ON ALL VIEWS IN SCHEMA sustainability_db.published TO ROLE sustainability_published_r;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA sustainability_db.published TO ROLE sustainability_published_r;

{%- endif %}

USE ROLE {{ defaultRole }};