-- ------------------------------------------------------------------------------------------------------------------------------
-- create view for assets 
-- ------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW published.vergesense_assets AS
SELECT 
        model_id,
        capability_id,
        capability_name,
        asset_id,
        asset_name,
        model_id_asset, 
        space_id,
        space_name,
        space_type,
        seating_capacity,
        tenant_name,
        tenant_id,
        tenant_unit_id,
        tenant_unit_name,
        floor_id,
        level_id,
        level_name,
        floor_sort_order,
        trend_id,
        building_id,
        building_name,
        site_id,
        site_name,
        time_zone
FROM  transformed.vergesense_assets;