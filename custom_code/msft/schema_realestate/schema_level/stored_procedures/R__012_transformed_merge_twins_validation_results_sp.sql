-- ------------------------------------------------------------------------------------------------------------------------------
-- Tasks that consume stage streams and move data into 'transformed' layer

-- ------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE transformed.merge_twins_validation_results_sp()
    RETURNS STRING
    LANGUAGE SQL
  AS
    $$
        MERGE INTO transformed.twins_validation_results AS tgt 
        USING (
            SELECT
            json_value:TwinId::STRING AS twin_id,
            json_value:ModelId::STRING AS model_id,
            json_value:TwinInfo::VARIANT AS twin_info,
            json_value:CheckTime::STRING AS check_time,
            json_value:Description::STRING AS description,
            json_value:ResultType::STRING AS result_type,
            json_value:RuleId::STRING AS rule_id,
            json_value:ResultInfo::VARIANT AS result_info,
            json_value:RuleScope::VARIANT AS rule_scope,
            json_value:BatchTime::STRING AS batch_time,
            TRY_PARSE_JSON(json_value:Raw::VARIANT)::variant AS raw_json_value,
            true AS is_active,
            _stage_record_id,
            json_value:_loader_run_id::STRING AS _loader_run_id,
            json_value:_ingested_at::TIMESTAMP_NTZ AS _ingested_at,
            _staged_at
            FROM raw.json_twins_validation_results_str
            -- Make sure that the joining key is unique (take just the latest batch if there is more)
            QUALIFY ROW_NUMBER() OVER (PARTITION BY twin_id,batch_time ORDER BY batch_time DESC) = 1
        ) AS src
            ON (tgt.twin_id = src.twin_id)
		   AND (tgt.batch_time = src.batch_time)
        WHEN MATCHED THEN
            UPDATE 
            SET 
            tgt.model_id = src.model_id,
            tgt.twin_info = src.twin_info,
            tgt.check_time = src.check_time,
            tgt.description = src.description,
            tgt.result_type = src.result_type,
            tgt.rule_id = src.rule_id,
            tgt.result_info = src.result_info,
            tgt.rule_scope = src.rule_scope,
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
            twin_id,
            model_id,
            twin_info,
            check_time,
            description,
            result_type,
            rule_id,
            result_info,
            rule_scope,
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
            src.twin_id,
            src.model_id,
            src.twin_info,
            src.check_time,
            src.description,
            src.result_type,
            src.rule_id,
            src.result_info,
            src.rule_scope,
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