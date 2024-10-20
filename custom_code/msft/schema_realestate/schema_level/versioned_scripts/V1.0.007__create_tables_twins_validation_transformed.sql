-- ------------------------------------------------------------------------------------------------------------------------------
-- Create tables for digital commissioning validation
-- ------------------------------------------------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS  transformed.twins_validation_results(
	twin_id 			VARCHAR(100),
    model_id 			VARCHAR(100),
    twin_info 		    VARIANT,
	check_time 			TIMESTAMP_NTZ(9),
	description 		VARCHAR(1000),
	result_type 		VARCHAR(100),
    rule_id 		    VARCHAR(255),
    result_info	   		VARIANT,
    rule_scope	    	VARIANT,
	batch_time 			TIMESTAMP_NTZ(9),
    raw_json_value		VARIANT,
    _is_active	     	BOOLEAN,
	_stage_record_id 	VARCHAR(36),
	_loader_run_id 		VARCHAR(36),
    _created_at 		TIMESTAMP_NTZ(9),
    _last_updated_at 	TIMESTAMP_NTZ(9),
	_ingested_at 		TIMESTAMP_NTZ(9),
	_staged_at 			TIMESTAMP_NTZ(9)
);

CREATE TABLE IF NOT EXISTS  transformed.twins_validation_aggregate_scores(
	id							VARCHAR(100),
	model_id					VARCHAR(100),
	average_attribute_score		DECIMAL(18,2),
	average_relationship_score	DECIMAL(18,2),  
	batch_time 					TIMESTAMP_NTZ(9),
    raw_json_value				VARIANT,
    _is_active	     			BOOLEAN,
	_stage_record_id 			VARCHAR(36),
	_loader_run_id 				VARCHAR(36),
    _created_at 				TIMESTAMP_NTZ(9),
    _last_updated_at 			TIMESTAMP_NTZ(9),
	_ingested_at 				TIMESTAMP_NTZ(9),
	_staged_at 					TIMESTAMP_NTZ(9)
);

CREATE TABLE IF NOT EXISTS  transformed.twins_static_validation_scores(
	twin_id 			VARCHAR(100),
	batch_time 			TIMESTAMP_NTZ(9),
    twin_info	   		VARIANT,
    model_id 			VARCHAR(100),
	attribute_score 	INTEGER,
	relationship_score	INTEGER,
    raw_json_value		VARIANT,
    _is_active	     	BOOLEAN,
	_stage_record_id 	VARCHAR(36),
	_loader_run_id 		VARCHAR(36),
    _created_at 		TIMESTAMP_NTZ(9),
    _last_updated_at 	TIMESTAMP_NTZ(9),
	_ingested_at 		TIMESTAMP_NTZ(9),
	_staged_at 			TIMESTAMP_NTZ(9)
);

CREATE TABLE IF NOT EXISTS  transformed.twins_validation_connectivity_scores(
	twin_id 			VARCHAR(100),
	batch_time 			TIMESTAMP_NTZ(9),
    twin_info	   		VARIANT,
    model_id 			VARCHAR(100),
	connectivity_score 	INTEGER,
    raw_json_value		VARIANT,
    _is_active	     	BOOLEAN,
	_stage_record_id 	VARCHAR(36),
	_loader_run_id 		VARCHAR(36),
    _created_at 		TIMESTAMP_NTZ(9),
    _last_updated_at 	TIMESTAMP_NTZ(9),
	_ingested_at 		TIMESTAMP_NTZ(9),
	_staged_at 			TIMESTAMP_NTZ(9)
);