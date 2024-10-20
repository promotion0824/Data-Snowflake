-- ******************************************************************************************************************************
-- Stored procedure that merges into ontology_models; full refresh of the table
-- This is called via transformed.merge_ontology_models_tk which is scheduled by CRON
-- USAGE:  CALL transformed.merge_ontology_models_sp(SYSTEM$CURRENT_USER_TASK_NAME());

-- Upon deployment, we need to make sure the table is immmediately populated by running the script at the end of this file:
-- ******************************************************************************************************************************

CREATE OR REPLACE PROCEDURE transformed.merge_ontology_models_sp(task_name VARCHAR)
    RETURNS STRING
    LANGUAGE SQL
  AS
    $$
    BEGIN

      -- create a new transient table with the latest data
      -- we are not using the stream because we do not want to delete data from transformed if stream is empty
      -- we are still using the stream as a method of only executing the task when stream has data.
      CREATE OR REPLACE TRANSIENT TABLE transformed.new_ontology_models AS
      SELECT * 
      FROM raw.json_ontology_models
      QUALIFY ROW_NUMBER() OVER (PARTITION BY json_value:Id::STRING  ORDER BY json_value:ExportTime::TIMESTAMP_NTZ(9) DESC) = 1;

      -- clear the stream since we have the data in transient table above.
      CREATE OR REPLACE TEMPORARY TABLE transformed.ontology_models_str_persisted AS
      SELECT top 1 * FROM raw.json_ontology_models_str;

      -- delete records that are no longer in the source (we don't need this because ADX doesn't allow deletes anwyay)
      -- MERGE INTO transformed.ontology_models AS tgt 
      -- USING (
      -- SELECT
      --         tgt.id
      -- FROM transformed.ontology_models tgt
      -- LEFT JOIN transformed.new_ontology_models src ON tgt.id = src.json_value:Id::STRING
      -- WHERE src.json_value:Id::STRING IS NULL
      --   ) AS src
      -- ON (tgt.id = src.id)
      -- WHEN MATCHED THEN DELETE;

      -- update or insert records that are in the source
      BEGIN TRANSACTION;
      MERGE INTO transformed.ontology_models AS tgt 
		  USING (
			SELECT
              json_value:Id::STRING AS id,
              json_value:IsDecommissioned::STRING AS is_decommissioned,
              json_value:ExportTime::TIMESTAMP_NTZ(9) AS export_time,
              json_value:DisplayName::STRING AS display_name,
              json_value:ModelDefinition::VARIANT AS model_definition,
              json_value:Deleted::BOOLEAN AS deleted,
              json_value:allExtends::STRING AS all_extends,
              HASH(model_definition,all_extends) AS hash_key
        FROM transformed.new_ontology_models
        WHERE IFNULL(deleted,false) = false
        QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY export_time DESC) = 1
        ) AS src
			ON (tgt.id = src.id) 
		  WHEN MATCHED AND src.hash_key != HASH(tgt.model_definition,tgt.all_extends) THEN
			UPDATE 
			SET 
			  tgt.is_decommissioned = src.is_decommissioned,
        tgt.export_time = src.export_time,
        tgt.display_name = src.display_name,
        tgt.model_definition = src.model_definition,
        tgt.deleted = src.deleted,
        tgt.all_extends = src.all_extends
		  WHEN NOT MATCHED THEN
			INSERT (
        id,
        is_decommissioned,
        export_time,
        display_name,
        model_definition,
        deleted,
        all_extends
        )
			VALUES (
        src.id,
        src.is_decommissioned,
        src.export_time,
        src.display_name,
        src.model_definition,
        src.deleted,
        src.all_extends
        );

    COMMIT;
    END;
    $$
;


-- execute upon deployment
CALL transformed.merge_ontology_models_sp('initial deployment');