-- ******************************************************************************************************************************
-- Stored procedure that merges into workflow_core_tickets
-- This is called via transformed.merge_workflow_core_tickets_tk which is scheduled by CRON
-- USAGE:  CALL transformed.merge_workflow_core_tickets_sp(SYSTEM$CURRENT_USER_TASK_NAME());
-- ******************************************************************************************************************************

CREATE OR REPLACE PROCEDURE transformed.merge_workflow_core_tickets_sp(task_name VARCHAR)
    RETURNS STRING
    LANGUAGE SQL
  AS
    $$
    	BEGIN
        MERGE INTO transformed.workflow_core_tickets AS tgt 
        USING (
          SELECT
            json_value:Id::STRING AS id,
            json_value:CustomerId::STRING AS customer_id,
            json_value:SiteId::STRING AS site_id,
            json_value:FloorCode::STRING AS floor_code,
            json_value:SequenceNumber::STRING AS sequence_number,
            json_value:Priority::STRING AS priority,
            json_value:Status::STRING AS status,
            json_value:status_description::STRING AS status_description,
            json_value:IssueType::STRING AS issue_type,
            json_value:IssueId::STRING AS issue_id,
            json_value:IssueName::STRING AS issue_name,
            json_value:Description::STRING AS description,
            json_value:Cause::STRING AS cause,
            json_value:Solution::STRING AS solution,
            json_value:ReporterId::STRING AS reporter_id,
            json_value:ReporterName::STRING AS reporter_name,
            json_value:ReporterPhone::STRING AS reporter_phone,
            json_value:ReporterEmail::STRING AS reporter_email,
            json_value:ReporterCompany::STRING AS reporter_company,
            json_value:AssigneeId::STRING AS assignee_id,
            json_value:AssigneeName::STRING AS assignee_name,
            json_value:DueDate::STRING AS due_date,
            json_value:CreatedDate::STRING AS created_date,
            json_value:UpdatedDate::STRING AS up_dated_date,
            json_value:ResolvedDate::STRING AS resolved_date,
            json_value:ClosedDate::STRING AS closed_date,
            json_value:SourceType::STRING AS source_type,
            json_value:SourceId::STRING AS source_id,
            json_value:SourceName::STRING AS source_name,
            json_value:ExternalId::STRING AS external_id,
            json_value:Externalstatus::STRING AS external_status,
            json_value:ExternalMetadata::STRING AS external_metadata,
            json_value:Summary::STRING AS summary,
            json_value:AssigneeType::STRING AS assignee_type,
            json_value:InsightId::STRING AS insight_id,
            json_value:InsightName::STRING AS insight_name,
            json_value:Latitude::DECIMAL(9,6) AS latitude,
            json_value:Longitude::DECIMAL(9,6) AS longitude,
            json_value:CreatorId::STRING AS creator_id,
            json_value:Occurrence::STRING AS occurrence,
            json_value:ScheduledDate::STRING AS scheduled_date,
            json_value:Notes::STRING AS notes,
            json_value:ExternalCreatedDate::STRING AS external_created_date,
            json_value:ExternalUpdatedDate::STRING AS external_updated_date,
            json_value:LastUpdatedByExternalSource AS last_updated_by_external_source,
            json_value:CategoryId::STRING AS category_id,
            json_value:category::STRING AS category,
            json_value:IsTemplate::STRING AS is_template,
            json_value:TemplateId::STRING AS template_id,
            json_value:recurrence::STRING AS recurrence,
            json_value:overduethreshold::STRING AS overdue_threshold,
            json_value:assets::ARRAY AS assets,
            json_value:tasks::ARRAY AS tasks,
            json_value:attachments::ARRAY AS attachments,
            json_value:datavalue::STRING AS data_value,
            json_value::VARIANT AS raw_json_value,
            true AS is_active,
            _stage_record_id,
            json_value:_loader_run_Id::STRING AS _loader_run_id,
            json_value:_ingested_at::TIMESTAMP_NTZ AS _ingested_at,
            _staged_at
          FROM raw.json_workflow_core_tickets_str
          -- Make sure that the joining key is unique (take just the latest batch if there is more)
          QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _ingested_at DESC) = 1
        ) AS src
          ON (tgt.id = src.id)
        WHEN MATCHED THEN
          UPDATE 
          SET 
            tgt.customer_id=src.customer_id,
            tgt.site_id=src.site_id,
            tgt.floor_code=src.floor_code,
            tgt.sequence_number=src.sequence_number,
            tgt.priority=src.priority,
            tgt.status=src.status,
            tgt.status_description=src.status_description,
            tgt.issue_type=src.issue_type,
            tgt.issue_id=src.issue_id,
            tgt.issue_name=src.issue_name,
            tgt.description=src.description,
            tgt.cause=src.cause,
            tgt.solution=src.solution,
            tgt.reporter_id=src.reporter_id,
            tgt.reporter_name=src.reporter_name,
            tgt.reporter_phone=src.reporter_phone,
            tgt.reporter_email=src.reporter_email,
            tgt.reporter_company=src.reporter_company,
            tgt.assignee_id=src.assignee_id,
            tgt.assignee_name=src.assignee_name,
            tgt.due_date=src.due_date,
            tgt.created_date=src.created_date,
            tgt.up_dated_date=src.up_dated_date,
            tgt.resolved_date=src.resolved_date,
            tgt.closed_date=src.closed_date,
            tgt.source_type=src.source_type,
            tgt.source_id=src.source_id,
            tgt.source_name=src.source_name,
            tgt.external_id=src.external_id,
            tgt.external_status=src.external_status,
            tgt.external_metadata=src.external_metadata,
            tgt.summary=src.summary,
            tgt.assignee_type=src.assignee_type,
            tgt.insight_id=src.insight_id,
            tgt.insight_name=src.insight_name,
            tgt.latitude=src.latitude,
            tgt.longitude=src.longitude,
            tgt.creator_id=src.creator_id,
            tgt.occurrence=src.occurrence,
            tgt.scheduled_date=src.scheduled_date,
            tgt.notes=src.notes,
            tgt.external_created_date=src.external_created_date,
            tgt.external_updated_date=src.external_updated_date,
            tgt.last_updated_by_external_source=src.last_updated_by_external_source,
            tgt.category_id=src.category_id,
            tgt.category=src.category,
            tgt.is_template=src.is_template,
            tgt.template_id=src.template_id,
            tgt.recurrence=src.recurrence,
            tgt.overdue_threshold=src.overdue_threshold,
            tgt.assets=src.assets,
            tgt.tasks=src.tasks,
            tgt.attachments=src.attachments,
            tgt.data_value=src.data_value,
            tgt.raw_json_value =src.raw_json_value ,
            tgt._is_active = true,
            tgt._last_updated_at = SYSDATE(),
            tgt._stage_record_id =src._stage_record_id ,
            tgt._loader_run_id =src._loader_run_id ,
            tgt._ingested_at =src._ingested_at ,
            tgt._staged_at =src._staged_at
        WHEN NOT MATCHED THEN
          INSERT (
            id,
            customer_id,
            site_id,
            floor_code,
            sequence_number,
            priority,
            status,
            status_description,
            issue_type,
            issue_id,
            issue_name,
            description,
            cause,
            solution,
            reporter_id,
            reporter_name,
            reporter_phone,
            reporter_email,
            reporter_company,
            assignee_id,
            assignee_name,
            due_date,
            created_date,
            up_dated_date,
            resolved_date,
            closed_date,
            source_type,
            source_id,
            source_name,
            external_id,
            external_status,
            external_metadata,
            summary,
            assignee_type,
            insight_id,
            insight_name,
            latitude,
            longitude,
            creator_id,
            occurrence,
            scheduled_date,
            notes,
            external_created_date,
            external_updated_date,
            last_updated_by_external_source,
            category_id,
            category,
            is_template,
            template_id,
            recurrence,
            overdue_threshold,
            assets,
            tasks,
            attachments,
            data_value,
            raw_json_value,
            _is_active,
            _created_at,
            _last_updated_at,
            _stage_record_id,
            _loader_run_id,
            _ingested_at,
            _staged_at
          ) 
          VALUES (
            src.id,
            src.customer_id,
            src.site_id,
            src.floor_code,
            src.sequence_number,
            src.priority,
            src.status,
            src.status_description,
            src.issue_type,
            src.issue_id,
            src.issue_name,
            src.description,
            src.cause,
            src.solution,
            src.reporter_id,
            src.reporter_name,
            src.reporter_phone,
            src.reporter_email,
            src.reporter_company,
            src.assignee_id,
            src.assignee_name,
            src.due_date,
            src.created_date,
            src.up_dated_date,
            src.resolved_date,
            src.closed_date,
            src.source_type,
            src.source_id,
            src.source_name,
            src.external_id,
            src.external_status,
            src.external_metadata,
            src.summary,
            src.assignee_type,
            src.insight_id,
            src.insight_name,
            src.latitude,
            src.longitude,
            src.creator_id,
            src.occurrence,
            src.scheduled_date,
            src.notes,
            src.external_created_date,
            src.external_updated_date,
            src.last_updated_by_external_source,
            src.category_id,
            src.category,
            src.is_template,
            src.template_id,
            src.recurrence,
            src.overdue_threshold,
            src.assets,
            src.tasks,
            src.attachments,
            src.data_value,
            src.raw_json_value ,
            true,
            SYSDATE(),
            SYSDATE(),
            src._stage_record_id,
            src._loader_run_id,
            src._ingested_at,
            src._staged_at
          );
        -- Clear the stream;
		    CREATE OR REPLACE temporary table transformed.dummy AS SELECT TOP 1 * FROM raw.json_workflow_core_tickets_str;
	    END;
    $$
    ;
        -- Populate the table upon deployment
        CALL transformed.merge_workflow_core_tickets_sp('deployment');

