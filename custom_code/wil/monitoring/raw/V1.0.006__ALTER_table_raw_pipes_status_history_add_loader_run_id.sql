-- ******************************************************************************************************************************
-- Check for existence of columns, add if not exists
-- ******************************************************************************************************************************
  
EXECUTE IMMEDIATE $$
  DECLARE
    column1_exists boolean;

  BEGIN
    column1_exists := (
      SELECT IFF(COUNT(*) = 1, true, false) 
      FROM central_monitoring_db.information_schema.columns 
      WHERE table_schema ilike 'raw' AND table_name ilike 'pipes_status_history' AND column_name ilike '_loader_run_id'
	  );

  
    IF(NOT(column1_exists)) THEN 
      BEGIN
        ALTER TABLE central_monitoring_db.raw.pipes_status_history ADD COLUMN _loader_run_id VARCHAR(36) NULL;
      END;
    END IF;
	
  END;
$$
