-- ------------------------------------------------------------------------------------------------------------------------------
-- Create AAD users
-- ------------------------------------------------------------------------------------------------------------------------------
USE ROLE USERADMIN;
CREATE USER IF NOT EXISTS mpampena
  LOGIN_NAME   = 'mpampena@willowinc.com'
  DISPLAY_NAME = 'Matt'
  FIRST_NAME   = 'Matt'
  LAST_NAME    = 'Pampena' 
  EMAIL        = 'mpampena@willowinc.com'
  DEFAULT_ROLE = data_engineer
  DEFAULT_WAREHOUSE = data_engineer_wh;

CREATE USER IF NOT EXISTS tgottwald
  LOGIN_NAME   = 'tgottwald@willowinc.com'
  DISPLAY_NAME = 'Tomas'
  FIRST_NAME   = 'Tomas'
  LAST_NAME    = 'Gottwald' 
  EMAIL        = 'tgottwald@willowinc.com'
  DEFAULT_ROLE = data_engineer
  DEFAULT_WAREHOUSE = data_engineer_wh;

CREATE USER IF NOT EXISTS pcostello
  LOGIN_NAME   = 'pcostello@willowinc.com'
  DISPLAY_NAME = 'Pat'
  FIRST_NAME   = 'Pat'
  LAST_NAME    = 'Costello' 
  EMAIL        = 'pcostello@willowinc.com'
  DEFAULT_ROLE = data_scientist
  DEFAULT_WAREHOUSE = data_scientist_wh;

CREATE USER IF NOT EXISTS xwang
  LOGIN_NAME   = 'xwang@willowinc.com'
  DISPLAY_NAME = 'Xi'
  FIRST_NAME   = 'Xi'
  LAST_NAME    = 'Wang' 
  EMAIL        = 'xwang@willowinc.com'
  DEFAULT_ROLE = bi_developer
  DEFAULT_NAMESPACE = 'prd_db.published'
  DEFAULT_WAREHOUSE = bi_developer_wh;

CREATE USER IF NOT EXISTS lnsantos
  LOGIN_NAME   = 'lnsantos@willowinc.com'
  DISPLAY_NAME = 'Enzo'
  FIRST_NAME   = 'Enzo'
  LAST_NAME    = 'Santos' 
  EMAIL        = 'lnsantos@willowinc.com'
  DEFAULT_ROLE = bi_developer
  DEFAULT_NAMESPACE = 'prd_db.published'
  DEFAULT_WAREHOUSE = bi_developer_wh;

CREATE USER IF NOT EXISTS dtarekegne
  LOGIN_NAME   = 'dtarekegne@willowinc.com'
  DISPLAY_NAME = 'Dawit'
  FIRST_NAME   = 'Dawit'
  LAST_NAME    = 'Tarekegne' 
  EMAIL        = 'dtarekegne@willowinc.com'
  DEFAULT_ROLE = analyst
  DEFAULT_NAMESPACE = 'prd_db.published'
  DEFAULT_WAREHOUSE = analyst_wh; 

CREATE USER IF NOT EXISTS mburke
  LOGIN_NAME   = 'mburke@willowinc.com'
  DISPLAY_NAME = 'Michael'
  FIRST_NAME   = 'Michael'
  LAST_NAME    = 'Burke' 
  EMAIL        = 'mburke@willowinc.com'
  DEFAULT_ROLE = digital_engineer
  DEFAULT_NAMESPACE = 'prd_db.published'
  DEFAULT_WAREHOUSE = digital_engineer_wh;
GRANT ROLE digital_engineer TO USER mburke;

CREATE USER IF NOT EXISTS ecalzavara
  LOGIN_NAME   = 'ecalzavara@willowinc.com'
  DISPLAY_NAME = 'Enrico'
  FIRST_NAME   = 'Enrico'
  LAST_NAME    = 'Calzavara' 
  EMAIL        = 'ecalzavara@willowinc.com'
  DEFAULT_ROLE = digital_engineer
  DEFAULT_NAMESPACE = 'prd_db.published'
  DEFAULT_WAREHOUSE = digital_engineer_wh;

CREATE USER IF NOT EXISTS bblack
  LOGIN_NAME   = 'bblack@willowinc.com'
  DISPLAY_NAME = 'Brandon'
  FIRST_NAME   = 'Brandon'
  LAST_NAME    = 'Black' 
  EMAIL        = 'bblack@willowinc.com'
  DEFAULT_ROLE = performance_engineer
  DEFAULT_NAMESPACE = 'prd_db.published'
  DEFAULT_WAREHOUSE = performance_engineer_wh;

CREATE USER IF NOT EXISTS tbendavid
  LOGIN_NAME   = 'tbendavid@willowinc.com'
  DISPLAY_NAME = 'Tom'
  FIRST_NAME   = 'Tom'
  LAST_NAME    = 'Ben-David' 
  EMAIL        = 'tbendavid@willowinc.com'
  DEFAULT_ROLE = performance_engineer
  DEFAULT_NAMESPACE = 'prd_db.published'
  DEFAULT_WAREHOUSE = performance_engineer_wh;

CREATE USER IF NOT EXISTS cmanna
  LOGIN_NAME   = 'cmanna@willowinc.com'
  DISPLAY_NAME = 'Chris'
  FIRST_NAME   = 'Chris'
  LAST_NAME    = 'Manna' 
  EMAIL        = 'cmanna@willowinc.com'
  DEFAULT_ROLE = performance_engineer
  DEFAULT_NAMESPACE = 'prd_db.published'
  DEFAULT_WAREHOUSE = performance_engineer_wh;

CREATE USER IF NOT EXISTS wroantree
  LOGIN_NAME   = 'wroantree@willowinc.com'
  DISPLAY_NAME = 'Will'
  FIRST_NAME   = 'Will'
  LAST_NAME    = 'Roantree' 
  EMAIL        = 'wroantree@willowinc.com'
  DEFAULT_ROLE = performance_engineer
  DEFAULT_NAMESPACE = 'prd_db.published'
  DEFAULT_WAREHOUSE = performance_engineer_wh;

CREATE USER IF NOT EXISTS imercer
  LOGIN_NAME   = 'imercer@willowinc.com'
  DISPLAY_NAME = 'Ian'
  FIRST_NAME   = 'Ian'
  LAST_NAME    = 'Mercer' 
  EMAIL        = 'imercer@willowinc.com'
  DEFAULT_ROLE = performance_engineer
  DEFAULT_NAMESPACE = 'prd_db.published'
  DEFAULT_WAREHOUSE = performance_engineer_wh;

CREATE USER IF NOT EXISTS rszcodronski
  LOGIN_NAME   = 'rszcodronski@willowinc.com'
  DISPLAY_NAME = 'Rick'
  FIRST_NAME   = 'Rick'
  LAST_NAME    = 'Szcodronski' 
  EMAIL        = 'rszcodronski@willowinc.com'
  DEFAULT_ROLE = performance_engineer
  DEFAULT_NAMESPACE = 'prd_db.published'
  DEFAULT_WAREHOUSE = performance_engineer_wh;
CREATE USER IF NOT EXISTS hneiman
  LOGIN_NAME   = 'hneiman@willowinc.com'
  DISPLAY_NAME = 'Haley'
  FIRST_NAME   = 'Haley'
  LAST_NAME    = 'Neiman' 
  EMAIL        = 'hneiman@willowinc.com'
  DEFAULT_ROLE = performance_engineer
  DEFAULT_NAMESPACE = 'prd_db.published'
  DEFAULT_WAREHOUSE = performance_engineer_wh;

CREATE USER IF NOT EXISTS igilurrutia
  LOGIN_NAME   = 'igilurrutia@willowinc.com'
  DISPLAY_NAME = 'Ignacio'
  FIRST_NAME   = 'Ignacio'
  LAST_NAME    = 'Gil Urrutia' 
  EMAIL        = 'igilurrutia@willowinc.com'
  DEFAULT_ROLE = performance_engineer
  DEFAULT_NAMESPACE = 'prd_db.published'
  DEFAULT_WAREHOUSE = performance_engineer_wh;

CREATE USER IF NOT EXISTS nberk
  LOGIN_NAME   = 'nberk@willowinc.com'
  DISPLAY_NAME = 'Nathaniel'
  FIRST_NAME   = 'Nathaniel'
  LAST_NAME    = 'Berk' 
  EMAIL        = 'nberk@willowinc.com'
  DEFAULT_ROLE = performance_engineer
  DEFAULT_NAMESPACE = 'prd_db.published'
  DEFAULT_WAREHOUSE = performance_engineer_wh;

CREATE USER IF NOT EXISTS rbharati
  LOGIN_NAME   = 'rbharati@willowinc.com'
  DISPLAY_NAME = 'Ritika'
  FIRST_NAME   = 'Ritika'
  LAST_NAME    = 'Bhharati' 
  EMAIL        = 'rbharati@willowinc.com'
  DEFAULT_ROLE = performance_engineer
  DEFAULT_NAMESPACE = 'prd_db.published'
  DEFAULT_WAREHOUSE = performance_engineer_wh;

CREATE USER IF NOT EXISTS ezhu
  LOGIN_NAME   = 'ezhu@willowinc.com'
  DISPLAY_NAME = 'Elliot'
  FIRST_NAME   = 'Elliot'
  LAST_NAME    = 'Zhu' 
  EMAIL        = 'ezhu@willowinc.com'
  DEFAULT_ROLE = data_scientist
  DEFAULT_WAREHOUSE = data_scientist_wh;

CREATE USER IF NOT EXISTS amclachlan
  LOGIN_NAME   = 'amclachlan@willowinc.com'
  DISPLAY_NAME = 'Andrew'
  FIRST_NAME   = 'Andrew'
  LAST_NAME    = 'McLachlan' 
  EMAIL        = 'amclachlan@willowinc.com'
  DEFAULT_ROLE = data_engineer
  DEFAULT_WAREHOUSE = data_engineer_wh;
CREATE USER IF NOT EXISTS sbedwell
  LOGIN_NAME   = 'sbedwell@willowinc.com'
  DISPLAY_NAME = 'Scott'
  FIRST_NAME   = 'Scott'
  LAST_NAME    = 'Bedwell' 
  EMAIL        = 'sbedwell@willowinc.com'
  DEFAULT_ROLE = data_engineer
  DEFAULT_WAREHOUSE = data_engineer_wh;

{% if hasDsSandboxDb -%}

CREATE USER IF NOT EXISTS csimpson
  LOGIN_NAME   = 'csimpson@willowinc.com'
  DISPLAY_NAME = 'Christopher'
  FIRST_NAME   = 'Christopher'
  LAST_NAME    = 'Simpson' 
  EMAIL        = 'csimpson@willowinc.com'
  DEFAULT_ROLE = ds_sandbox_streamlit_viewer
  DEFAULT_WAREHOUSE = data_scientist_wh;
 
{%- endif %}

CREATE USER IF NOT EXISTS jgarner
  LOGIN_NAME   = 'jgarner@willowinc.com'
  DISPLAY_NAME = 'Joshua Garner'
  FIRST_NAME   = 'Joshua'
  LAST_NAME    = 'Garner' 
  EMAIL        = 'jgarner@willowinc.com'
  DEFAULT_ROLE = performance_engineer
  DEFAULT_NAMESPACE = 'prd_db.published'
  DEFAULT_WAREHOUSE = performance_engineer_wh;
  
USE ROLE {{ defaultRole }};
