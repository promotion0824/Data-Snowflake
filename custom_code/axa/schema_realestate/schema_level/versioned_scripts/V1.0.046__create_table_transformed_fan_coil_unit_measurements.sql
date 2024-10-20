---------------------------------------------------------------------------------------
-- Create table for storing pre-aggregated data
-- ------------------------------------------------------------------------------------

CREATE OR REPLACE TRANSIENT TABLE transformed.fan_coil_unit_measurements (
	asset_id 				VARCHAR(255),
	site_id 				VARCHAR(36),
	date_local 				DATE,
	date_time_local_15min 	TIMESTAMP_NTZ(9),
	mode_sensor 			FLOAT,
	zone_air_temperature 	FLOAT,
	sample_count 			NUMBER(38,0),
	max_timestamp_local 	TIMESTAMP_NTZ(9),
	max_enqueued_at_utc 	TIMESTAMP_NTZ(9),
	_created_at 			TIMESTAMP_NTZ(9) DEFAULT CAST(CONVERT_TIMEZONE('UTC', CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_TZ(9))) AS TIMESTAMP_NTZ(9)),
	_created_by_task 		VARCHAR(255),
	_last_updated_at 		TIMESTAMP_NTZ(9) DEFAULT CAST(CONVERT_TIMEZONE('UTC', CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_TZ(9))) AS TIMESTAMP_NTZ(9)),
	_last_updated_by_task 	VARCHAR(255)
);