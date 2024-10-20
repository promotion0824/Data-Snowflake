-- ------------------------------------------------------------------------------------------------------------------------------
-- create views
-- ------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE VIEW published.twins_validation_results AS
SELECT 
	v.twin_id,
	v.model_id,
	TRY_PARSE_JSON((v.twin_info)::variant):FloorId::STRING AS floor_id,
	TRY_PARSE_JSON((v.twin_info)::variant):SiteId::STRING AS site_id,
	l.level_name,
	l.site_name,
	TRY_PARSE_JSON((v.result_info)::variant):actualCount::STRING AS actual_count,
	TRY_PARSE_JSON((v.result_info)::variant):expectedCount::STRING AS expected_count,
	TRY_PARSE_JSON((v.result_info)::variant)::STRING AS result_info,
	TRY_PARSE_JSON((v.rule_scope)::variant)::STRING AS rule_scope,
	TRY_PARSE_JSON((v.twin_info)::variant)::STRING AS twin_info,
	v.batch_time,
	v.check_time
FROM transformed.twins_validation_results v
    LEFT JOIN transformed.levels_buildings l
           ON (site_id = l.site_id)
          AND (l.floor_id = TRY_PARSE_JSON((v.twin_info)::variant):FloorId::STRING)
;

CREATE OR REPLACE VIEW published.twins_validation_aggregate_scores AS
SELECT 
	v.id,
	t.name,
	v.model_id,
	v.average_attribute_score,
	v.average_relationship_score,
	v.batch_time
FROM transformed.twins_validation_aggregate_scores v
LEFT JOIN transformed.twins t
       ON (v.id = t.unique_id)
;

CREATE OR REPLACE VIEW published.twins_static_validation_scores AS
SELECT 
	v.twin_id,
	v.model_id,
	v.batch_time,
	v.attribute_score,
	v.relationship_score,
	TRY_PARSE_JSON((v.twin_info)::variant):FloorId::STRING AS floor_id,
	TRY_PARSE_JSON((v.twin_info)::variant):SiteId::STRING AS site_id,
	TRY_PARSE_JSON((v.twin_info)::variant) AS twin_info,
	l.level_name,
	s.name AS site_name,
	l.floor_sort_order
FROM transformed.twins_static_validation_scores v
    LEFT JOIN transformed.levels_buildings l
           ON (site_id = l.site_id)
          AND (l.floor_id = TRY_PARSE_JSON((v.twin_info)::variant):FloorId::STRING)
    LEFT JOIN transformed.sites s
           ON (s.site_id = TRY_PARSE_JSON((v.twin_info)::variant):SiteId::STRING)
;

CREATE OR REPLACE VIEW published.twins_validation_connectivity_scores AS
SELECT 
	v.twin_id,
	v.model_id,
	v.batch_time,
	v.connectivity_score,
	TRY_PARSE_JSON((v.twin_info)::variant):FloorId::STRING AS floor_id,
	TRY_PARSE_JSON((v.twin_info)::variant):SiteId::STRING AS site_id,
	TRY_PARSE_JSON((v.twin_info)::variant) AS twin_info,
	l.level_name,
	s.name AS site_name,
	l.floor_sort_order
FROM transformed.twins_validation_connectivity_scores v
    LEFT JOIN transformed.levels_buildings l
           ON (site_id = l.site_id)
          AND (l.floor_id = TRY_PARSE_JSON((v.twin_info)::variant):FloorId::STRING)
    LEFT JOIN transformed.sites s
           ON (s.site_id = TRY_PARSE_JSON((v.twin_info)::variant):SiteId::STRING)
;
		  
		  