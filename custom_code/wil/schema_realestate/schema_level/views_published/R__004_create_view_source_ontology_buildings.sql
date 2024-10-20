

CREATE OR REPLACE VIEW published.source_ontology_buildings AS
SELECT 
  path,
  key_value,
  file_name,
  _ingested_at
FROM raw.stage_ontology
WHERE file_name = 'ontology/opendigitaltwins-building.csv'
;