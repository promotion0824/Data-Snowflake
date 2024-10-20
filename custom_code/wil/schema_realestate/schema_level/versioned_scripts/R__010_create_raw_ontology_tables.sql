-- ******************************************************************************************************************************
-- Create raw ontology tables
-- ******************************************************************************************************************************

CREATE TABLE IF NOT EXISTS raw.stage_ontology (
	path ARRAY,
	key_value VARIANT,
	file_name VARCHAR(1000),
	_ingested_at TIMESTAMP_NTZ DEFAULT SYSDATE()
);