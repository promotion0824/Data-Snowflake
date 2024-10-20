-- ********************************************************************************************************************************
-- Create view
-- ********************************************************************************************************************************
CREATE OR REPLACE VIEW published.building_scopes AS

SELECT 
    account_name,
    building_id,
    model_id,
    scope_id,
    site_id,
    customer_id
FROM transformed.building_scopes
;
