-- ******************************************************************************************************************************
-- Stored procedure that merges into insights
-- This is called via transformed.merge_insights_tk which is scheduled by CRON
-- USAGE:  CALL transformed.merge_insights_sp(SYSTEM$CURRENT_USER_TASK_NAME());
-- ******************************************************************************************************************************

CREATE OR REPLACE PROCEDURE transformed.merge_insights_sp(task_name VARCHAR)
    RETURNS STRING
    LANGUAGE SQL
  AS
    $$
    BEGIN
      CREATE OR REPLACE TABLE transformed.insights AS

        SELECT DISTINCT 
          json_value:Id::STRING AS id,
          json_value:SiteId::STRING AS site_id,
          json_value:SequenceNumber::STRING AS sequence_number,
          json_value:EquipmentId::STRING AS equipment_id,
          json_value:ExternalId::STRING AS external_id,
          json_value:ExternalMetadata::VARIANT AS external_metadata,
          json_value:Type::INTEGER AS type,
          json_value:Name::STRING AS name,	  
          json_value:Description::STRING AS description,
          json_value:Priority::INTEGER AS priority,
          json_value:Status::INTEGER AS status,
          json_value:ExternalStatus::STRING AS external_status,
          json_value:CreatedDate::STRING AS created_date,
          json_value:UpdatedDate::STRING AS updated_date,
          TRY_TO_TIMESTAMP(json_value:LastOccurredDate::STRING) AS last_occurred_date,
          TRY_TO_TIMESTAMP(json_value:DetectedDate::STRING) AS detected_date,
          json_value:SourceType::INTEGER AS source_type,
          json_value:SourceId::STRING AS source_id,
          json_value:RuleId::STRING AS rule_id,
          json_value:RuleName::STRING AS rule_name,
          json_value:TwinId::STRING AS twin_id,
          json_value:TwinName::STRING AS twin_name,
          json_value:PrimaryModelId::STRING AS primary_model_id,
          json_value:PointsJson::VARIANT AS points_json,
          json_value:Recommendation::STRING AS recommendation,
          json_value:Reported::BOOLEAN AS reported,
          json_value:State::INTEGER AS state,
          json_value:NewOccurrence::BOOLEAN AS new_occurrence,
          json_value:OccurrenceCount::INTEGER AS occurrence_count,
          json_value:CreatedUserId::STRING AS created_user_id,
          json_value:CustomerId::STRING AS customer_id,
          json_value::VARIANT AS raw_json_value,
          SYSDATE() AS _created_at,
          SYSDATE() AS _last_updated_at,
          _stage_record_id,
          json_value:_loader_run_Id::STRING AS _loader_run_id,
          json_value:_ingested_at::TIMESTAMP_NTZ AS _ingested_at,
          _staged_at
        FROM raw.json_insights
        QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _ingested_at DESC) = 1;

        -- Clear out the stream;
        CREATE OR REPLACE TEMPORARY TABLE raw.trash AS SELECT TOP 1 * FROM raw.json_insights_str;

    END
    $$
;