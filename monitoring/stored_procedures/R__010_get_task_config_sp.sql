 -- ----------------------------------------------------------------------------------------------------------------------------
 -- This procedure is used to get most recent task configuration for monitoring purposes
 -- ----------------------------------------------------------------------------------------------------------------------------
 
USE DATABASE monitoring_db;

	CREATE OR REPLACE PROCEDURE transformed.get_task_config_sp()
		RETURNS TABLE (
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
		)
	LANGUAGE SQL
	AS
	$$  
		DECLARE
			res1 RESULTSET default (show tasks in raw);
            res2 RESULTSET default (show tasks in transformed);
			vw_table RESULTSET;
		BEGIN
			vw_table := (EXECUTE IMMEDIATE 'SELECT "name" as task_name,"database_name","schema_name","state","schedule","predecessors","condition","owner","definition","comment","warehouse" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) 
											UNION ALL
											SELECT "name" as task_name,"database_name","schema_name","state","schedule","predecessors","condition","owner","definition","comment","warehouse" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID(-2))) 
                         '
                        );
			RETURN TABLE(vw_table);
		END
	$$;
