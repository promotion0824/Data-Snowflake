-- ------------------------------------------------------------------------------------------------------------------------------
-- Create streams
-- ------------------------------------------------------------------------------------------------------------------------------

CREATE STREAM IF NOT EXISTS raw.stage_data_loader_str
    ON TABLE raw.stage_data_loader
    APPEND_ONLY = TRUE
    SHOW_INITIAL_ROWS = TRUE;

CREATE STREAM IF NOT EXISTS raw.stage_telemetry_str
    ON TABLE raw.stage_telemetry
    APPEND_ONLY = TRUE
    SHOW_INITIAL_ROWS = TRUE;

CREATE STREAM IF NOT EXISTS raw.json_directory_core_sites_str 
    ON TABLE raw.json_directory_core_sites
    APPEND_ONLY = TRUE
    SHOW_INITIAL_ROWS = TRUE;
	
CREATE STREAM IF NOT EXISTS raw.json_inspections_str 
    ON TABLE raw.json_inspections
    APPEND_ONLY = TRUE
    SHOW_INITIAL_ROWS = TRUE;

CREATE STREAM IF NOT EXISTS raw.json_impact_scores_str 
    ON TABLE raw.json_impact_scores
    APPEND_ONLY = TRUE
    SHOW_INITIAL_ROWS = TRUE;

CREATE STREAM IF NOT EXISTS raw.json_site_core_floors_str 
    ON TABLE raw.json_site_core_floors
    APPEND_ONLY = TRUE
    SHOW_INITIAL_ROWS = TRUE;

CREATE STREAM IF NOT EXISTS raw.custom_customer_stage_str 
    ON TABLE raw.custom_customer_stage
    APPEND_ONLY = TRUE
    SHOW_INITIAL_ROWS = TRUE;

CREATE STREAM IF NOT EXISTS raw.json_twins_str
    ON TABLE raw.json_twins
    APPEND_ONLY = TRUE
    SHOW_INITIAL_ROWS = TRUE;

CREATE STREAM IF NOT EXISTS raw.json_twins_relationships_str
    ON TABLE raw.json_twins_relationships
    APPEND_ONLY = TRUE
    SHOW_INITIAL_ROWS = TRUE;
