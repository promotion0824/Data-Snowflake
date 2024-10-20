-- ******************************************************************************************************************************
-- Stored procedure that merges into impact_scores table
-- This is called via transformed.merge_impact_scores_tk which is scheduled after merge_insights_tk
-- ******************************************************************************************************************************

CREATE OR REPLACE PROCEDURE transformed.merge_impact_scores_sp(task_name VARCHAR)
    RETURNS STRING
    LANGUAGE SQL
  AS
    $$
    BEGIN
      CREATE OR REPLACE TABLE transformed.impact_scores AS
      SELECT
        json_value:ExternalId::STRING AS external_id,
        json_value:FieldId::STRING AS field_id,
        json_value:Id::STRING AS id,
        json_value:InsightId::STRING AS insight_id,
        json_value:Name::STRING AS name,
        json_value:RuleId::STRING AS rule_id,
        json_value:Unit::STRING AS unit,
        json_value:Value::FLOAT AS value,
        SYSDATE() AS _created_at,
        SYSDATE() AS _last_updated_at,
        _stage_record_id,
        json_value:_loader_run_Id::STRING AS _loader_run_id,
        json_value:_ingested_at::TIMESTAMP_NTZ AS _ingested_at,
        _staged_at
      FROM raw.json_impact_scores
      QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _ingested_at DESC) = 1;

      CREATE OR REPLACE TEMPORARY TABLE raw.trash AS SELECT TOP 1 * FROM raw.json_impact_scores_str;
    END;
    $$
;
