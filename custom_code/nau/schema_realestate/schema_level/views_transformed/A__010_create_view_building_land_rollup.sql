-- ******************************************************************************************************************************
-- Create view
-- ******************************************************************************************************************************
CREATE VIEW transformed.building_land_rollup AS
SELECT DISTINCT 
    b.building_id AS land_id,
    bs.building_id,
    b2.gross_area,
    b2.gross_area_unit,
    b2.time_zone
FROM transformed.buildings b
CROSS JOIN transformed.building_scopes bs
JOIN transformed.buildings b2 ON bs.building_id = b2.building_id
WHERE CONTAINS(bs.scope_id, b.building_id)
 AND b.model_id = 'dtmi:com:willowinc:Land;1'
;