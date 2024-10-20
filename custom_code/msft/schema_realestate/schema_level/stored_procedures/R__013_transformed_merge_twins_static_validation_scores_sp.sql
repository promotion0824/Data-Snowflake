-- ------------------------------------------------------------------------------------------------------------------------------
-- Tasks that consume stage streams and move data into 'transformed' layer

-- ------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE transformed.merge_twins_static_validation_scores_sp()
    RETURNS STRING
    LANGUAGE SQL
  AS
    $$
        MERGE INTO transformed.twins_static_validation_scores AS tgt 
        USING (
            SELECT
            json_value:TwinId::STRING AS twin_id,
            json_value:BatchTime::STRING AS batch_time,
            json_value:TwinInfo::VARIANT AS twin_info,
            json_value:ModelId::STRING AS model_id,
            json_value:AttributeScore::INTEGER AS attribute_score,
            json_value:RelationshipScore::INTEGER AS relationship_score,
            TRY_PARSE_JSON(json_value:Raw::VARIANT)::variant AS raw_json_value,
            true AS is_active,
            _stage_record_id,
            json_value:_loader_run_id::STRING AS _loader_run_id,
            json_value:_ingested_at::TIMESTAMP_NTZ AS _ingested_at,
            _staged_at
            FROM raw.json_twins_static_validation_scores_str
            -- Make sure that the joining key is unique (take just the latest batch if there is more)
            QUALIFY ROW_NUMBER() OVER (PARTITION BY twin_id,batch_time ORDER BY batch_time DESC) = 1
        ) AS src
            ON (tgt.twin_id = src.twin_id)
		   AND (tgt.batch_time = src.batch_time)
        WHEN MATCHED THEN
            UPDATE 
            SET 
            tgt.batch_time = src.batch_time,
            tgt.twin_info = src.twin_info,
            tgt.model_id = src.model_id,
            tgt.attribute_score = src.attribute_score,
            tgt.relationship_score = src.relationship_score,
            tgt.raw_json_value = src.raw_json_value,
            tgt._last_updated_at = SYSDATE(),
            tgt._is_active = true,
            tgt._stage_record_id = src._stage_record_id,
            tgt._loader_run_id = src._loader_run_id,
            tgt._ingested_at = src._ingested_at,
            tgt._staged_at = src._staged_at
        WHEN NOT MATCHED THEN
            INSERT (
            twin_id,
            batch_time,
            twin_info,
            model_id,
            attribute_score,
            relationship_score,
            raw_json_value,
            _is_active,
            _created_at,
            _last_updated_at,
            _stage_record_id, 
            _loader_run_id, 
            _ingested_at, 
            _staged_at)
            VALUES (
            src.twin_id,
            src.batch_time,
            src.twin_info,
            src.model_id,
            src.attribute_score,
            src.relationship_score,
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