CREATE OR REPLACE VIEW transformed.ontology_buildings AS
SELECT
    path AS path_string,
	NULLIF(SPLIT_PART(path_string,'>',1),'') AS model_level_1,
	NULLIF(SPLIT_PART(path_string,'>',2),'') AS model_level_2,
	NULLIF(SPLIT_PART(path_string,'>',3),'') AS model_level_3,
	NULLIF(SPLIT_PART(path_string,'>',4),'') AS model_level_4,
	NULLIF(SPLIT_PART(path_string,'>',5),'') AS model_level_5,
	NULLIF(SPLIT_PART(path_string,'>',6),'') AS model_level_6,
	NULLIF(SPLIT_PART(path_string,'>',7),'') AS model_level_7,
	NULLIF(SPLIT_PART(path_string,'>',8),'') AS model_level_8,
	NULLIF(SPLIT_PART(path_string,'>',9),'') AS model_level_9,
	NULLIF(SPLIT_PART(path_string,'>',10),'') AS model_level_10,
	model_id,
	extends_model_id,
	display_name_en AS display_name,
	type,
	context,
	display_names_all,
	contents,
	extends,
	path_models,
	path,
	model_id AS id
FROM transformed.ontology_model_hierarchy r
ORDER BY model_level_1,model_level_2,model_level_3,model_level_4,model_level_5,model_level_6,model_level_7,model_level_8,model_level_9,model_level_10
;


CREATE OR REPLACE VIEW published.ontology_buildings AS
SELECT * FROM transformed.ontology_buildings
;
