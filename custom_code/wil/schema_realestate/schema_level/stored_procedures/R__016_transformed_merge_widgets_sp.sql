-- ******************************************************************************************************************************
-- Stored procedure that merges into insights
-- This is called via transformed.merge_insights_tk which is scheduled by CRON
-- USAGE:  CALL transformed.merge_insights_sp(SYSTEM$CURRENT_USER_TASK_NAME());
-- ******************************************************************************************************************************

CREATE OR REPLACE PROCEDURE transformed.merge_widgets_sp(task_name VARCHAR)
    RETURNS STRING
    LANGUAGE SQL
    EXECUTE AS CALLER
  AS
    $$
  MERGE INTO transformed.widgets AS tgt 
  USING (
    SELECT
      json_value:SiteId::STRING AS site_id,
      json_value:PortfolioId::STRING AS portfolio_id,
      json_value:CustomerId::STRING AS customer_id,
      json_value:WidgetId::STRING AS widget_id,
      json_value:Position::INTEGER AS position,
      json_value:Type::INTEGER AS type,
      TRY_PARSE_JSON(json_value:Metadata::VARIANT)::variant AS metadata,
      _stage_record_id,
      json_value:_loader_run_Id::STRING AS _loader_run_id,
      json_value:_ingested_at::TIMESTAMP_NTZ AS _ingested_at,
      _staged_at
    FROM raw.json_widgets_str
    -- Make sure that the joining key is unique (take just the latest batch if there is more)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY widget_id, site_id, portfolio_id ORDER BY _ingested_at DESC) = 1
  ) AS src
    ON (tgt.widget_id = src.widget_id)
   AND (tgt.site_id = src.site_id)
    AND (tgt.portfolio_id = src.portfolio_id) 
  WHEN MATCHED AND src.metadata != tgt.metadata THEN
    UPDATE 
    SET 
		tgt.site_id = src.site_id,
    tgt.portfolio_id = src.portfolio_id,
    tgt.customer_id = src.customer_id,
		tgt.position = src.position,
		tgt.type = src.type,
		tgt.metadata = src.metadata,
		tgt._last_updated_at = SYSDATE(),
		tgt._loader_run_id = src._loader_run_id,
		tgt._ingested_at = src._ingested_at,
		tgt._staged_at = src._staged_at
  WHEN NOT MATCHED THEN
    INSERT (
		site_id,
    portfolio_id,
    customer_id,
		widget_id,
		position,
		type,
		metadata,
		_created_at,
		_last_updated_at,
		_loader_run_id,
		_ingested_at,
		_staged_at
	  ) 
    VALUES (
		src.site_id,
    src.portfolio_id,
    src.customer_id, 
		src.widget_id,
		src.position,
		src.type,
		src.metadata,
		SYSDATE(),
		SYSDATE(),
		src._loader_run_id,
    src._ingested_at,
		src._staged_at
    );
    $$
;