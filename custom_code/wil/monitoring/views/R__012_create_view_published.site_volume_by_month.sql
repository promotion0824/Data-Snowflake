-- ******************************************************************************************************************************
-- Create view
-- ******************************************************************************************************************************
CREATE OR REPLACE VIEW central_monitoring_db.published.site_volume_by_month AS
WITH cte_customer_abbreviation AS (
       SELECT DISTINCT 
              sv.customer_abbreviation,
              cust.customer_name
       FROM central_monitoring_db.transformed.site_volume_by_month sv
       LEFT JOIN prd_db.transformed.site_core_sites s
              ON (s.id = sv.site_id )
       LEFT JOIN prd_db.transformed.customers cust
              ON (s.customer_id = cust.customer_id)
       WHERE customer_name IS NOT NULL
)
SELECT
    month_start_date,
    COALESCE(cust.customer_name,ca.customer_name) AS customer_name,
    cust.portfolio_name,
    s.id AS site_id,
    s.name AS site_name,
    sv.connector_id,
    c.name AS connector_name,
    sv.count_of_telemetry_points,
    sv.count_of_rows,
    sv.building_id,
    sv.building_name,
    sv.type,
    s.type AS site_type,
    s.area AS gross_area_from_site_core,
    sv.gross_area AS gross_area_from_twin,
    sv.rentable_area AS rentable_area_from_twin,
    sv.model_id,
    sv.customer_abbreviation,
    s.source_created_date AS site_core_created_date,
    sv.snowflake_created_date
    FROM prd_db.transformed.site_core_sites s
    LEFT JOIN prd_db.transformed.customers cust
           ON (s.customer_id = cust.customer_id)
    LEFT JOIN central_monitoring_db.transformed.site_volume_by_month sv
           ON (s.id = sv.site_id )
    LEFT JOIN prd_db.transformed.connectors c 
           ON (sv.site_id = c.site_id )
          AND (sv.connector_id = c.id)
    LEFT JOIN cte_customer_abbreviation ca
           ON (sv.customer_abbreviation = ca.customer_abbreviation)    
    WHERE sv.month_start_date <= SYSDATE()
;