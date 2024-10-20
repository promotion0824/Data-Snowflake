-- ******************************************************************************************************************************
-- Create view 
-- ******************************************************************************************************************************
CREATE OR REPLACE VIEW transformed.tenant_served_by_twin AS
-- assets to tenant units
SELECT
    c.name AS tenant_name,
    c.twin_id AS tenant_id,
    t.twin_id AS tenant_unit_id,
    t.name AS tenant_unit_name,
    tr.relationship_name,
    tr.target_twin_id AS asset_id,
    tr._last_updated_at AS relationship_last_updated_at,
    t._last_updated_at AS tenant_last_updated_at
FROM transformed.twins_relationships_deduped tr
JOIN transformed.twins t
  ON (tr.source_twin_id = t.twin_id)
JOIN transformed.twins_relationships_deduped tr2
  ON (tr2.source_twin_id = t.twin_id)
JOIN transformed.twins_relationships_deduped tr3
  ON (tr3.source_twin_id = tr2.target_twin_id)
JOIN transformed.twins c
  ON (tr3.target_twin_id = c.twin_id)
WHERE 
    IFNULL(t.is_deleted,FALSE) = FALSE
AND IFNULL(c.is_deleted,FALSE) = FALSE
AND IFNULL(tr.is_deleted,FALSE) = FALSE
AND IFNULL(tr2.is_deleted,FALSE) = FALSE
AND IFNULL(tr3.is_deleted,FALSE) = FALSE
AND tr.relationship_name IN ('servedBy')
AND t.model_id = 'dtmi:com:willowinc:TenantUnit;1'
AND tr2.relationship_name = 'hasLease'
AND tr3.relationship_name = 'leasee'

UNION ALL
-- space to tenant units
SELECT
    c.name AS tenant_name,
    c.twin_id AS tenant_id,
    t.twin_id AS tenant_unit_id,
    t.name AS tenant_unit_name,
    tr.relationship_name,
    tr.source_twin_id AS asset_id,
    tr._last_updated_at AS relationship_last_updated_at,
    t._last_updated_at AS tenant_last_updated_at
FROM transformed.twins_relationships_deduped tr
JOIN transformed.twins t
  ON (tr.target_twin_id = t.twin_id)
JOIN transformed.twins_relationships_deduped tr2
  ON (tr2.source_twin_id = t.twin_id)
JOIN transformed.twins_relationships_deduped tr3
  ON (tr3.source_twin_id = tr2.target_twin_id)
JOIN transformed.twins c
  ON (tr3.target_twin_id = c.twin_id)
WHERE 
    IFNULL(t.is_deleted,FALSE) = FALSE
AND IFNULL(c.is_deleted,FALSE) = FALSE
AND IFNULL(tr.is_deleted,FALSE) = FALSE
AND IFNULL(tr2.is_deleted,FALSE) = FALSE
AND IFNULL(tr3.is_deleted,FALSE) = FALSE
AND tr.relationship_name IN ('includedIn','locatedIn')
AND t.model_id = 'dtmi:com:willowinc:TenantUnit;1'
AND tr2.relationship_name = 'hasLease'
AND tr3.relationship_name = 'leasee'
;