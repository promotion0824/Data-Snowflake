-- ******************************************************************************************************************************
-- Stored procedure that merges into insight_occurrences
-- This is called via transformed.merge_insight_occurrences_tk which is scheduled by CRON
-- USAGE:  CALL transformed.merge_insight_occurrences_sp(SYSTEM$CURRENT_USER_TASK_NAME());
-- ******************************************************************************************************************************

CREATE OR REPLACE PROCEDURE transformed.merge_insight_occurrences_sp(task_name VARCHAR)
    RETURNS STRING
    LANGUAGE SQL
  AS
    $$
  MERGE INTO transformed.insight_occurrences AS tgt 
  USING (
    SELECT
      json_value:Id::STRING AS id,
      json_value:InsightId::STRING AS insight_id,
      json_value:OccurrenceId::STRING AS occurrence_id,
      json_value:IsFaulted::STRING AS is_faulted,
      json_value:IsValid::STRING AS is_valid,
      json_value:Started::STRING AS started,
      json_value:Ended::STRING AS ended,
      json_value:text::STRING AS text,
      json_value::VARIANT AS raw_json_value,
      true AS is_active,
      _stage_record_id,
      json_value:_loader_run_Id::STRING AS _loader_run_id,
      json_value:_ingested_at::TIMESTAMP_NTZ AS _ingested_at,
      _staged_at
    FROM raw.json_insight_occurrences_str
    -- Make sure that the joining key is unique (take just the latest batch if there is more)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _ingested_at DESC) = 1
  ) AS src
    ON (tgt.id = src.id)
  WHEN MATCHED THEN
    UPDATE 
    SET 
      tgt.insight_id = src.insight_id,
      tgt.occurrence_id = src.occurrence_id,
      tgt.is_faulted = src.is_faulted,
      tgt.is_valid = src.is_valid,
      tgt.started = src.started,
      tgt.ended = src.ended,
      tgt.text = src.text,
      tgt.raw_json_value = src.raw_json_value,
      tgt._last_updated_at = SYSDATE(),
      tgt._stage_record_id = src._stage_record_id,
      tgt._loader_run_id = src._loader_run_id,
      tgt._ingested_at = src._ingested_at,
      tgt._staged_at = src._staged_at
  WHEN NOT MATCHED THEN
    INSERT (
      id,
      insight_id,
      occurrence_id,
      is_faulted,
      is_valid,
      started,
      ended,
      text,
      raw_json_value,
      _created_at,
      _last_updated_at,
      _stage_record_id,
      _loader_run_id,
      _ingested_at,
      _staged_at
	  ) 
    VALUES (
      src.id,
      src.insight_id,
      src.occurrence_id,
      src.is_faulted,
      src.is_valid,
      src.started,
      src.ended,
      src.text,
      src.raw_json_value,
      SYSDATE(),
      SYSDATE(),
      src._stage_record_id,
      src._loader_run_id,
      src._ingested_at,
      src._staged_at
    );
    $$
;
