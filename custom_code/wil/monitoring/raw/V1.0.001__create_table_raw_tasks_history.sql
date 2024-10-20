-- ------------------------------------------------------------------------------------------------------------------------------
-- Create raw task history table
-- ------------------------------------------------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS central_monitoring_db.raw.tasks_history (
    account_name 		TEXT,
    query_id			TEXT,
    name				TEXT,
    database_name		TEXT,
    schema_name			TEXT,
    query_text			TEXT,
    condition_text		TEXT,
    state				TEXT,
    error_code			NUMBER,
    error_message		TEXT,
    scheduled_time		TIMESTAMP_TZ,
    query_start_time	TIMESTAMP_TZ,
    completed_time		TIMESTAMP_TZ,
    root_task_id		TEXT,
    graph_version		NUMBER,
    run_id				NUMBER,
    return_value		TEXT,
    _exported_at 		TIMESTAMP_LTZ
);
