-- ------------------------------------------------------------------------------------------------------------------------------
-- Create tables for directory core sql table

-- ------------------------------------------------------------------------------------------------------------------------------
CREATE TRANSIENT TABLE IF NOT EXISTS raw.json_workflow_core_tickets(
    _stage_record_id 	VARCHAR(36),
	 json_value 		VARIANT,
    _stage_file_name 	VARCHAR(1000),
	_loader_run_id 		VARCHAR(36),
	_ingested_at 		TIMESTAMP_NTZ(9),
    _staged_at 			TIMESTAMP_NTZ(9)
);

CREATE STREAM IF NOT EXISTS raw.json_workflow_core_tickets_str 
    ON TABLE raw.json_workflow_core_tickets
    APPEND_ONLY = TRUE
    SHOW_INITIAL_ROWS = TRUE;

CREATE OR REPLACE TABLE transformed.workflow_core_tickets(
    id							VARCHAR(36),
    customer_id					VARCHAR(36),
    site_id						VARCHAR(36),
    floor_code					VARCHAR(128),
    sequence_number				VARCHAR(128),
    priority					INT,
    status						INT,
    status_description          VARCHAR(128),
    issue_type					INT,
    issue_id					VARCHAR(36),
    issue_name					VARCHAR(128),
    description					VARCHAR(16777216),
    cause						VARCHAR(16777216),
    solution					VARCHAR(16777216),
    reporter_id					VARCHAR(36),
    reporter_name				VARCHAR(128),
    reporter_phone				VARCHAR(64),
    reporter_email				VARCHAR(128),
    reporter_company			VARCHAR(128),
    assignee_id					VARCHAR(36),
    assignee_name				VARCHAR(128),
    due_date					TIMESTAMP_NTZ(9),
    created_date				TIMESTAMP_NTZ(9),
    up_dated_date				TIMESTAMP_NTZ(9),
    resolved_date				TIMESTAMP_NTZ(9),
    closed_date					TIMESTAMP_NTZ(9),
    source_type					INT,
    source_id					VARCHAR(36),
    source_name					VARCHAR(128),
    external_id					VARCHAR(256),
    external_status				VARCHAR(128),
    external_metadata			VARCHAR(16777216),
    summary						VARCHAR(16777216),
    assignee_type				INT,
    insight_id					VARCHAR(36),
    insight_name				VARCHAR(1000),
    latitude					DECIMAL(9,6),
    longitude					DECIMAL(9,6),
    creator_id					VARCHAR(36),
    occurrence					INT,
    scheduled_date				TIMESTAMP_NTZ(9),
    notes						VARCHAR(16777216),
    external_created_date		TIMESTAMP_NTZ(9),
    external_updated_date		TIMESTAMP_NTZ(9),
    last_updated_by_external_source BOOLEAN,
    category_id					VARCHAR(36),
    category					VARCHAR(255),
    is_template					BOOLEAN,
    template_id					VARCHAR(36),
    recurrence					VARCHAR(16777216),
    overdue_threshold			VARCHAR(64),
    assets						ARRAY,
    tasks						ARRAY,
    attachments					ARRAY,
    data_value					VARCHAR(16777216),
    raw_json_value 				VARIANT,
    _is_active 					BOOLEAN DEFAULT true,
    _created_at 				TIMESTAMP_NTZ DEFAULT SYSDATE(),
    _last_updated_at 			TIMESTAMP_NTZ DEFAULT SYSDATE(),
    _stage_record_id 			STRING,
    _loader_run_id 				VARCHAR(36),
    _ingested_at 				TIMESTAMP_NTZ,
    _staged_at 					TIMESTAMP_NTZ
);
