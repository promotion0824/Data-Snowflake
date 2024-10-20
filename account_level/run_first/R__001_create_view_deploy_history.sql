-- ------------------------------------------------------------------------------------------------------------------------------
-- Create view at account level
-- [env]_db.utils.deploy_history tracks deployments at the schema and custom code levels
-- ------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE VIEW util_db.public.deploy_history AS
-- Account level
	WITH cte_devops_pipeline_runs AS (
	SELECT 
		ops.pipeline_run_start,
		ops.pipeline_run_end,
		LEAD(pipeline_run_start, 1) OVER (ORDER BY pipeline_run_start) as pipeline_next_start,
		COALESCE(ops.pipeline_run_end,pipeline_next_start) AS pipeline_end_adjusted,
		ops.pipeline_name,
		ops.build_number,
		ops.snowflake_account,
		ops.env_variable,
		ops.queued_by,
		ops.trigger_event,
		ops.source_branch,
		ops.commit_comment
	FROM util_db.public.devops_pipeline_runs ops
)
-- Account level
	SELECT 
		ch.installed_on,
		CONVERT_TIMEZONE('UTC', 'America/Los_Angeles',ch.installed_on) AS installed_on_pst,
		CONVERT_TIMEZONE('UTC', 'Australia/Sydney',ch.installed_on) AS installed_on_aet,
		ops.pipeline_name,
		ops.build_number,
		ops.snowflake_account,
		ops.env_variable,
		ops.queued_by,
		ops.trigger_event,
		'account_level' AS deploy_type, 
		ch.script, 
		ch.status, 
		ch.execution_time,
		ops.source_branch,
		ops.commit_comment
	FROM util_db.schemachange.change_history ch
		JOIN cte_devops_pipeline_runs ops 
		  ON (ch.installed_on BETWEEN ops.pipeline_run_start AND ops.pipeline_end_adjusted)
;