----------------------------------------------------------------------------------
-- Create view
-- One row per lowest level model in the hierarchy; includes full path to the top;
-- ------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE VIEW transformed.ontology_model_hierarchy AS
WITH cte_ontology_flattened AS (
    SELECT DISTINCT 
            b.id as model_id,
            --f.value::STRING extends_model_id,
            b.model_definition:extends[0]::STRING extends_model_id,
            IFNULL(b.model_definition:displayName:en::string, b.model_definition:displayName::string) AS display_name_en,
            b.model_definition:"@type"::string AS type,
            b.model_definition:"@context"::string AS context,
            b.model_definition:displayName AS display_names_all,
            b.model_definition:contents AS contents,
            b.model_definition:extends AS extends
    FROM transformed.ontology_models b
    --LATERAL FLATTEN(input => b.model_definition:extends) f
      WHERE b.Id ILIKE 'dtmi:com:willowinc%' 
        AND (extends_model_id ILIKE 'dtmi:com:willowinc%' OR extends_model_id ILIKE 'dtmi:digitaltwins:rec_3_3:core%' OR extends_model_id IS NULL)
        AND (b.deleted <> true OR b.deleted is null)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY b.id,extends_model_id ORDER BY context DESC) = 1
    UNION
    SELECT DISTINCT 
            b.id as model_id,
            model_definition:extends::STRING extends_model_id,
            IFNULL(b.model_definition:displayName:en::string, b.model_definition:displayName::string) AS display_name_en,
            b.model_definition:"@type"::string AS type,
            b.model_definition:"@context"::string AS context,
            b.model_definition:displayName AS display_names_all,
            b.model_definition:contents AS contents,
            b.model_definition:extends AS extends
  FROM transformed.ontology_models b
        WHERE b.Id ILIKE 'dtmi:com:willowinc%' 
          AND b.id NOT LIKE 'dtmi:digitaltwins:rec_3_3%'
          AND model_definition:extends[0]::STRING is null
 	)
    , cte_path AS (
      SELECT
        *,  SYS_CONNECT_BY_PATH(model_id, '>') AS path_models,
        REPLACE(REPLACE(SUBSTRING(path_models,2,16000),'dtmi:com:willowinc:',''),';1','') AS path
      FROM cte_ontology_flattened 
        WHERE (extends_model_id NOT LIKE 'dtmi:digitaltwins:rec_3_3:%' AND model_id <> REPLACE(path,'>','') )
           OR extends_model_id IS NULL
        CONNECT BY extends_model_id = prior model_id
    )
   SELECT * FROM cte_path 
   QUALIFY ROW_NUMBER() OVER (PARTITION BY model_id,REPLACE(extends_model_id,'[]',null) ORDER BY length(path) DESC) = 1
;