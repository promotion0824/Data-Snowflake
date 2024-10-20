-- ******************************************************************************************************************************
-- Load ontology
-- ******************************************************************************************************************************

CREATE OR REPLACE PROCEDURE transformed.load_ontology_sp()
    RETURNS STRING
    LANGUAGE SQL
    EXECUTE AS CALLER
  AS
    $$
	  BEGIN

		TRUNCATE TABLE raw.stage_ontology;
		COPY INTO raw.stage_ontology (path,key_value,file_name,_ingested_at) FROM  (SELECT $1,$2,metadata$filename,SYSDATE() FROM @raw.ADHOC_ESG/ontology/)
			FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"' COMPRESSION = 'NONE') ON_ERROR = CONTINUE;

	END
    $$
;