-- ********************************************************************************************************************************
-- Create view
-- ********************************************************************************************************************************
CREATE OR REPLACE VIEW transformed.building_scopes_v AS
WITH 
 cte_customer AS (
    SELECT DISTINCT customer_id 
    FROM transformed.sites
    WHERE customer_id != '219c3081-b3dd-4ada-a56c-81d3dca21406'
    )
,cte_scope_twins AS (
    SELECT DISTINCT t1.site_id, t1.twin_id AS building_id, t1.model_id, o.extends_model_id, t1.twin_id, t1.name, tr1.relationship_name, tr1.target_twin_id
    FROM transformed.twins t1
    JOIN transformed.ontology_model_hierarchy_path o 
      ON (t1.model_id = o.model_id)
     AND o.path ilike '/dtmi:com:willowinc:Space;1/%'
    LEFT JOIN transformed.twins_relationships_deduped tr1 
      ON (t1.twin_id = tr1.source_twin_id) 
     AND (tr1.relationship_name = 'isPartOf')
    WHERE 
        t1.twin_id IS NOT NULL 
    AND IFNULL(t1.is_deleted,false) = false 
    AND IFNULL(tr1.is_deleted,false) = false
)
,cte_scope AS (
SELECT 
  CONNECT_BY_ROOT twin_id AS building_id,
  CONNECT_BY_ROOT model_id AS model_id,
  CONNECT_BY_ROOT site_id AS site_id,
  COALESCE(SYS_CONNECT_BY_PATH(twin_id, '/') || '/' || target_twin_id,building_id) AS scope_id
FROM cte_scope_twins t
START WITH model_id IN ('dtmi:com:willowinc:Building;1', 'dtmi:com:willowinc:OutdoorArea;1', 'dtmi:com:willowinc:Substructure;1', 'dtmi:com:willowinc:Land;1')
OR extends_model_id IN ('dtmi:com:willowinc:Building;1', 'dtmi:com:willowinc:OutdoorArea;1', 'dtmi:com:willowinc:Substructure;1', 'dtmi:com:willowinc:Land;1')
CONNECT BY twin_id = PRIOR target_twin_id
)
SELECT 
    LEFT(CURRENT_ACCOUNT_NAME(),
              COALESCE(
                  NULLIF(REGEXP_INSTR(CURRENT_ACCOUNT_NAME(), 'EU22'),0),
                  NULLIF(REGEXP_INSTR(CURRENT_ACCOUNT_NAME(), 'WEU'), 0),
                  NULLIF(REGEXP_INSTR(CURRENT_ACCOUNT_NAME(), 'AUE'), 0)
              ) -1
      ) AS account_name,
    building_id,
    model_id,
    scope_id || '/' AS scope_id,
    site_id,
    customer_id
FROM cte_scope
CROSS JOIN cte_customer
QUALIFY ROW_NUMBER() OVER (PARTITION BY building_id ORDER BY LENGTH(scope_id) DESC) = 1
;

CREATE OR REPLACE TABLE transformed.building_scopes AS SELECT * FROM transformed.building_scopes_v;