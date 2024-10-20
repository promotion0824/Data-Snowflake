-- ******************************************************************************************************************************
-- Create views
-- ******************************************************************************************************************************

-- raw
CREATE OR REPLACE VIEW raw.parsed_ontology_airports AS
SELECT
    REPLACE(r.path[0]::STRING,'Willow','') AS path_string,
	NULLIF(SPLIT_PART(path_string,'/',1),'') AS model_1,
	NULLIF(SPLIT_PART(path_string,'/',2),'') AS model_2,
	NULLIF(SPLIT_PART(path_string,'/',3),'') AS model_3,
	NULLIF(SPLIT_PART(path_string,'/',4),'') AS model_4,
	NULLIF(SPLIT_PART(path_string,'/',5),'') AS model_5,
	NULLIF(SPLIT_PART(path_string,'/',6),'') AS model_6,
	NULLIF(SPLIT_PART(path_string,'/',7),'') AS model_7,
	NULLIF(SPLIT_PART(path_string,'/',8),'') AS model_8,
	NULLIF(SPLIT_PART(path_string,'/',9),'') AS model_9,
	NULLIF(SPLIT_PART(path_string,'/',10),'') AS model_10,
	COALESCE(model_10,model_9,model_8,model_7,model_6,model_5,model_4,model_3,model_2) AS jsonFile,
	r.path AS path,
	key,
	value
FROM
 raw.stage_ontology r
,LATERAL flatten ( input => key_value )
WHERE file_name = 'ontology/opendigitaltwins-airport.csv';

-- published
CREATE OR REPLACE VIEW published.ontology_airports AS
SELECT  
	CASE WHEN model_1 ILIKE '%json' THEN NULL ELSE model_1 END AS model_level_1,
	CASE WHEN model_2 ILIKE '%json' THEN NULL ELSE model_2 END AS model_level_2,
	CASE WHEN model_3 ILIKE '%json' THEN NULL ELSE model_3 END AS model_level_3,
	CASE WHEN model_4 ILIKE '%json' THEN NULL ELSE model_4 END AS model_level_4,
	CASE WHEN model_5 ILIKE '%json' THEN NULL ELSE model_5 END AS model_level_5,
	CASE WHEN model_6 ILIKE '%json' THEN NULL ELSE model_6 END AS model_level_6,
	CASE WHEN model_7 ILIKE '%json' THEN NULL ELSE model_7 END AS model_level_7,
	CASE WHEN model_8 ILIKE '%json' THEN NULL ELSE model_8 END AS model_level_8,
	CASE WHEN model_9 ILIKE '%json' THEN NULL ELSE model_9 END AS model_level_9,
	CASE WHEN model_10 ILIKE '%json' THEN NULL ELSE model_10 END AS model_level_10,
	COALESCE(model_10,model_9,model_8,model_7,model_6,model_5,model_4,model_3,model_2) AS jsonFile,
	"'@id'"::STRING AS id,
	"'@context'"::STRING AS context,
	"'@type'"::STRING AS type,
	"'contents'"::VARIANT AS contents,
	"'displayName'"::VARIANT AS display_name,
	"'schemas'"::VARIANT AS schemas,
	"'description'"::VARIANT AS description
FROM raw.parsed_ontology_airports
    PIVOT(max(Value)
         FOR Key IN('@id','@context','@type','contents','displayName','schemas','description'))
        AS p
ORDER BY model_1,model_2,model_3,model_4,model_5,model_6,model_7,model_8,model_9,model_10
;