
-- ******************************************************************************************************************************
-- Create view 
-- ******************************************************************************************************************************
CREATE OR REPLACE VIEW transformed.tenant_lease AS

SELECT
    lb.site_name,
    tu.site_id,
    lb.building_name,
    lb.building_id,
    lb.level_name,
    lb.floor_sort_order,
    lb.id AS level_id,
    rm.model_id AS space_type,
    tr1.source_twin_id AS space_id,
    rm.raw_json_value:customProperties.area.grossArea AS gross_area,
    tu.name AS tenant_unit_name,
    tu.twin_id AS tenant_unit_id,
    tu.raw_json_value:customProperties.area.rentableArea AS rentable_area,
    c.name AS tenant_name,
    c.twin_id AS tenant_id,    
	ls.raw_json_value:customProperties.leaseStart::DATE AS lease_start,
	ls.raw_json_value:customProperties.leaseEnd::DATE AS lease_end,
    tr2.target_twin_id AS lease_id,
    ls.name AS lease_name, 
    tr1._last_updated_at AS relationship_last_updated_at,
    tu._last_updated_at AS tenant_last_updated_at
FROM transformed.twins_relationships_deduped tr1
JOIN transformed.twins tu -- TenantUnit
  ON (tr1.target_twin_id = tu.twin_id)
JOIN transformed.twins rm -- Room
  ON (tr1.source_twin_id = rm.twin_id)
JOIN transformed.twins_relationships_deduped tr2
  ON (tr2.source_twin_id = tu.twin_id)
JOIN transformed.twins ls -- Lease
  ON (tr2.target_twin_id = ls.twin_id)
JOIN transformed.twins_relationships_deduped tr3
  ON (tr3.source_twin_id = tr2.target_twin_id)
JOIN transformed.twins c -- Company
  ON (tr3.target_twin_id = c.twin_id)
LEFT JOIN transformed.levels_buildings lb
  ON (rm.floor_id = lb.floor_id)
WHERE 
    IFNULL(tu.is_deleted,FALSE) = FALSE
AND IFNULL(rm.is_deleted,FALSE) = FALSE
AND IFNULL(c.is_deleted,FALSE) = FALSE
AND IFNULL(tr1.is_deleted,FALSE) = FALSE
AND IFNULL(tr2.is_deleted,FALSE) = FALSE
AND IFNULL(tr3.is_deleted,FALSE) = FALSE
AND tr1.relationship_name IN ('includedIn')
AND tu.model_id = 'dtmi:com:willowinc:TenantUnit;1'
AND tr2.relationship_name = 'hasLease'
AND tr3.relationship_name = 'leasee'
order by site_name,level_id, space_id,tenant_unit_id
;