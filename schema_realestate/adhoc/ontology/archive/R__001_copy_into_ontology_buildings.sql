-- ******************************************************************************************************************************
-- Load ontology
-- This file needs something to change in order for schema change to load a new file; change the date
-- ontology file last updated: 2022-10-13
-- ******************************************************************************************************************************
USE {{ environment }}_db;

TRUNCATE TABLE raw.stage_ontology_buildings;
COPY INTO raw.stage_ontology_buildings FROM  @raw.adhoc_csv_sg/ontology_buildings.csv.gz ON_ERROR = CONTINUE;
