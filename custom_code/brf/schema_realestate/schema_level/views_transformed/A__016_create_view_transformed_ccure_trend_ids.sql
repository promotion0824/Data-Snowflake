-- ------------------------------------------------------------------------------------------------------------------------------
-- create View
-- ------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW transformed.ccure_trend_ids_v AS

WITH cte_new_site AS (
SELECT DISTINCT
	t.id AS capability_id, 
	capability_name,
	SPLIT_PART(t.id, '-',1)  || '-' || SPLIT_PART(t.id, '-',2) AS building_id,
	COALESCE(b.site_id, t.site_id) AS site_id, 
	b.site_name,
	b.building_name,
	t.time_zone
FROM transformed.capabilities_assets t
LEFT JOIN transformed.buildings b on b.building_id = SPLIT_PART(t.id, '-',1)  || '-' || SPLIT_PART(t.id, '-',2)
WHERE model_id_asset = 'dtmi:com:willowinc:Company;1'
	AND t.model_id in ('dtmi:com:willowinc:TotalEnteringPeopleCountSensor;1', 'dtmi:com:willowinc:UniqueEnteringPeopleCountSensor;1','dtmi:com:willowinc:PeopleCountSensor;1')
)
,cte_tenants AS (
	SELECT 
		t.site_id,
		s.building_id,
		t.twin_id AS tenant_unit_id,
		t.name AS tenant_unit_name,
		t.raw_json_value:customProperties:capacity.maxOccupancy AS max_occupancy,
		tr.target_twin_id AS lease_id,
		tt.name AS lease_name,
		tr2.target_twin_id AS company_id,
		t2.name AS company_name
	FROM transformed.twins t
	JOIN transformed.twins_relationships_deduped tr ON t.twin_id = tr.source_twin_id AND tr.relationship_name = 'hasLease'
	JOIN transformed.twins tt ON tr.target_twin_id = tt.twin_id AND tt.model_id = 'dtmi:com:willowinc:Lease;1'
	JOIN transformed.twins_relationships_deduped tr2 ON tt.twin_id = tr2.source_twin_id AND tr2.relationship_name = 'leasee'
	JOIN transformed.twins t2 ON t2.twin_id = tr2.target_twin_id AND t2.model_id = 'dtmi:com:willowinc:Company;1'
	JOIN transformed.sites s ON t.site_id = s.site_id
	WHERE t.model_id = 'dtmi:com:willowinc:TenantUnit;1'
	AND IFNULL(t.is_deleted,false) = false
	AND IFNULL(tr.is_deleted,false) = false
	AND IFNULL(tr2.is_deleted,false) = false
	AND IFNULL(t2.is_deleted,false) = false
	QUALIFY ROW_NUMBER() OVER (PARTITION BY t.twin_id ORDER BY t.export_time DESC) = 1
)
,cte_company AS (
	SELECT 
		building_id,
		company_id,
		SUM(max_occupancy) AS max_occupancy
	FROM cte_tenants
	GROUP BY 
	building_id,
	company_id
)
SELECT
	CASE WHEN ns.building_name IS NULL THEN c.building_id   ELSE ns.building_id   END AS building_id,
	CASE WHEN ns.building_name IS NULL THEN c.building_name ELSE ns.building_name END AS building_name,
	COALESCE(ns.site_id,c.site_id) AS site_id,
	COALESCE(ns.site_name,c.site_name) as site_name,
	c.time_zone,
	c.trend_id,
	c.id AS capability_id,
	c.description AS capability_description,
	c.model_id,
	c.capability_name,
	c.asset_id,
	c.asset_name, c.model_id_asset,
	SPLIT_PART(REPLACE(c.model_id_asset, ';',':'),':',4) AS category_name,
	COALESCE(cc.company_id,c.asset_id) AS company_id,
	CASE WHEN category_name = 'Company' THEN asset_name ELSE NULL END AS company_name,
	c.external_id,
	cc.max_occupancy
FROM transformed.capabilities_assets c
LEFT JOIN cte_new_site ns
	   ON (c.id = ns.capability_id)
LEFT JOIN cte_company cc
	   ON (cc.building_id = ns.building_id)
	  AND (cc.company_id = c.asset_id)
	WHERE (c.model_id in ('dtmi:com:willowinc:TotalEnteringPeopleCountSensor;1', 'dtmi:com:willowinc:UniqueEnteringPeopleCountSensor;1') 
     OR (c.model_id in ('dtmi:com:willowinc:PeopleCountSensor;1') AND c.capability_name IN ('Total People Count - C-Cure','Unique People Count - C-Cure')))
    AND c.model_id_asset in (
    'dtmi:com:willowinc:PeopleCountSensorEquipment;1',
    'dtmi:com:willowinc:Company;1',
    'dtmi:com:willowinc:Person;1',
    'dtmi:com:willowinc:Building;1'
    ) 
;
CREATE OR REPLACE TABLE transformed.ccure_trend_ids AS SELECT * FROM transformed.ccure_trend_ids_v;
