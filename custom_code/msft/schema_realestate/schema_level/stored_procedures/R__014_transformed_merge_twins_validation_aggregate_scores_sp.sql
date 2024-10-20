-- ------------------------------------------------------------------------------------------------------------------------------
-- Tasks that consume stage streams and move data into 'transformed' layer

-- ------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE transformed.merge_twins_validation_aggregate_scores_sp()
    RETURNS STRING
    LANGUAGE SQL
  AS
    $$
        MERGE INTO transformed.twins_validation_aggregate_scores AS tgt 
        USING (
            SELECT
            json_value:Id::STRING AS id,
            json_value:ModelId::STRING AS model_id,
            json_value:AverageAttributeScore::DECIMAL(18,2) AS average_attribute_score,
            json_value:AverageRelationshipScore::DECIMAL(18,2) AS average_relationship_score,
            json_value:BatchTime::TIMESTAMP_NTZ AS batch_time,
            TRY_PARSE_JSON(json_value:Raw::VARIANT)::variant AS raw_json_value,
            true AS is_active,
            _stage_record_id,
            json_value:_loader_run_id::STRING AS _loader_run_id,
            json_value:_ingested_at::TIMESTAMP_NTZ AS _ingested_at,
            _staged_at
            FROM raw.json_twins_validation_aggregate_scores_str
            -- Make sure that the joining key is unique (take just the latest batch if there is more)
            QUALIFY ROW_NUMBER() OVER (PARTITION BY id,batch_time ORDER BY batch_time DESC) = 1
        ) AS src
            ON (tgt.id = src.id)
		   AND (tgt.batch_time = src.batch_time)
        WHEN MATCHED THEN
            UPDATE 
            SET 
		    tgt.model_id = src.model_id,
	        tgt.average_attribute_score = src.average_attribute_score,
	        tgt.average_relationship_score = src.average_relationship_score, 
            tgt.batch_time = src.batch_time,
            tgt.raw_json_value = src.raw_json_value,
            tgt._last_updated_at = SYSDATE(),
            tgt._is_active = true,
            tgt._stage_record_id = src._stage_record_id,
            tgt._loader_run_id = src._loader_run_id,
            tgt._ingested_at = src._ingested_at,
            tgt._staged_at = src._staged_at
        WHEN NOT MATCHED THEN
            INSERT (
            id,
            model_id,
            average_attribute_score,
            average_relationship_score,
            batch_time,
            raw_json_value,
            _is_active,
            _created_at,
            _last_updated_at,
            _stage_record_id, 
            _loader_run_id, 
            _ingested_at, 
            _staged_at)
            VALUES (
	        src.id,
	        src.model_id,
	        src.average_attribute_score,
	        src.average_relationship_score,  
	        src.batch_time,
            src.raw_json_value,
            true,
            SYSDATE(), 
            SYSDATE(),
            src._stage_record_id, 
            src._loader_run_id, 
            src._ingested_at, 
            src._staged_at 
            );
    $$
;