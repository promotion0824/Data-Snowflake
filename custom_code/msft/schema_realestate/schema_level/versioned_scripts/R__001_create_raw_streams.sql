-- ------------------------------------------------------------------------------------------------------------------------------
-- Create streams
-- ------------------------------------------------------------------------------------------------------------------------------

CREATE STREAM IF NOT EXISTS raw.json_twins_validation_results_str
    ON TABLE raw.json_twins_validation_results
    APPEND_ONLY = TRUE
    SHOW_INITIAL_ROWS = TRUE;
	
CREATE STREAM IF NOT EXISTS raw.json_twins_validation_aggregate_scores_str
    ON TABLE raw.json_twins_validation_aggregate_scores
    APPEND_ONLY = TRUE
    SHOW_INITIAL_ROWS = TRUE;
	
CREATE STREAM IF NOT EXISTS raw.json_twins_static_validation_scores_str
    ON TABLE raw.json_twins_static_validation_scores
    APPEND_ONLY = TRUE
    SHOW_INITIAL_ROWS = TRUE;
	
CREATE STREAM IF NOT EXISTS raw.json_twins_validation_connectivity_scores_str
	ON TABLE raw.json_twins_validation_connectivity_scores
	APPEND_ONLY = TRUE
	SHOW_INITIAL_ROWS = TRUE;