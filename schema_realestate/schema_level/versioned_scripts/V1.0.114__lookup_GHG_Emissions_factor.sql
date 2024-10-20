----------------------------------------------------------------------------------
-- Create lookup table for emission factors
-- ------------------------------------------------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS transformed.lookup_emission_factor ( 
    environment_id   VARCHAR,
    name             VARCHAR, 
    site_id          VARCHAR,
    region           VARCHAR, 
    subregion        VARCHAR, 
    emissions_factor FLOAT, 
    unit             VARCHAR, 
    scope            VARCHAR 
    );
