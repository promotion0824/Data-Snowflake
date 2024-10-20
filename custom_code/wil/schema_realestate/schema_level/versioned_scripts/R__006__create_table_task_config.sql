 -- ----------------------------------------------------------------------------------------------------------------------------
 -- This table is for storing task configuration for monitoring purposes
 -- ----------------------------------------------------------------------------------------------------------------------------

USE DATABASE central_monitoring_db;

CREATE OR REPLACE TABLE raw.task_config (
            task_name 		VARCHAR(1000),
            database_name 	VARCHAR(36),
            schema_name 	VARCHAR(36),
            state 			VARCHAR(36),
            schedule 		VARCHAR(1000),
            predecessors 	VARCHAR(2000),
            condition 		VARCHAR(2000),
			owner			VARCHAR(36),
			definition		VARCHAR(16777216),
			comment			VARCHAR(2000),
			warehouse		VARCHAR(36)
);