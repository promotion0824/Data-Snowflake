-- ------------------------------------------------------------------------------------------------------------------------------
-- Grant roles to users
-- ------------------------------------------------------------------------------------------------------------------------------
USE ROLE USERADMIN;

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
GRANT ROLE performance_engineer TO USER jbass;
GRANT ROLE performance_engineer TO USER fwhitmore;

USE ROLE {{ defaultRole }};