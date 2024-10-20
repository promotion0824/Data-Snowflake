-- ******************************************************************************************************************************
-- Load ontology
-- Can also manually updload to dfw-yk93061-adhoc-stage/ontology
-- ******************************************************************************************************************************
USE &{db_name};

PUT file:///&{project_folder}/schema_realestate/adhoc/ontology/ontology_buildings.csv.gz @raw.adhoc_csv_sg AUTO_COMPRESS=TRUE OVERWRITE = TRUE;

