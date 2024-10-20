-- ******************************************************************************************************************************
-- Create view
-- ******************************************************************************************************************************
CREATE OR REPLACE VIEW published.billed_electricity AS
SELECT be.*, s.name as site_name, s.building_id, s.building_name
FROM transformed.billed_electricity be
JOIN transformed.sites s on be.site_id = s.site_id;