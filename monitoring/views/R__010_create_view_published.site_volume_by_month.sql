-- ******************************************************************************************************************************
-- Create view
-- ******************************************************************************************************************************

CREATE OR REPLACE VIEW monitoring_db.published.site_volume_by_month AS
WITH cte_db AS (
    SELECT SPLIT_PART(database_name,'_',2) AS customer_abbreviation,
    created AS snowflake_created_date
    FROM information_schema.databases
    WHERE STARTSWITH(database_name, '_')
    )
SELECT
    svm.month_start_date,
    svm.site_id,
    COALESCE(b.site_name,s.name) AS site_name,
    connector_id,
    svm.count_of_telemetry_points,
    svm.count_of_rows,
    b.building_id,
    COALESCE(b.building_name,s.name) AS building_name,
    b.type,
    b.gross_area,
    b.rentable_area,
    b.model_id,
    cte_db.customer_abbreviation,
    cte_db.snowflake_created_date,
    CURRENT_TIMESTAMP() AS _last_updated
FROM prd_db.transformed.site_volume_by_month svm
LEFT JOIN prd_db.transformed.buildings b
  ON svm.site_id = b.site_id
LEFT JOIN prd_db.transformed.sites s
  ON svm.site_id = s.site_id
CROSS JOIN cte_db
;