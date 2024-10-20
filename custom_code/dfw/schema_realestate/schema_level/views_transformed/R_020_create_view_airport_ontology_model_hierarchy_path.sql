CREATE OR REPLACE VIEW published.airport_ontology_model_hierarchy_path AS
	WITH cte_ontology_flattened AS (
      SELECT DISTINCT
        b.key_value:"@id"::string AS model_id,
        IFNULL(b.key_value:displayName:en::string, b.key_value:displayName::string) AS display_name_en,
        b.key_value:"@type"::string AS type,
        b.key_value:"@context"::string AS context,
        b.key_value:displayName AS display_names_all,
        b.key_value:contents AS contents,
        f.value::string AS extends_model_id,
        b.key_value:extends AS extends
      FROM raw.stage_ontology b,
      LATERAL FLATTEN(input => b.key_value:extends) f
      WHERE file_name = 'ontology/opendigitaltwins-airport.csv'
	)
	SELECT
		*  --, NULL as path
        ,SYS_CONNECT_BY_PATH(model_id, ' -> ') AS path
	FROM cte_ontology_flattened
	CONNECT BY 
		model_id = prior extends_model_id;