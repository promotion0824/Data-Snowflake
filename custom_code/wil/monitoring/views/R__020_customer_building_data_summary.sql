CREATE OR REPLACE VIEW central_monitoring_db.published.customer_building_data_summary AS
WITH cte_building_sum AS (
SELECT DISTINCT
    customer_name,
    site_name,
    building_name,
    s.type AS site_type,
    s.area AS gross_area_from_site_core,
    sv.gross_area AS gross_area_from_twin,
    sv.rentable_area AS rentable_area_from_twin,
    sv.connector_id,
    MAX(count_of_telemetry_points) OVER(PARTITION BY customer_name,site_name,building_name,s.type) AS NumberOfPointsPerBuilding,
    AVG(count_of_rows) OVER(PARTITION BY customer_name,site_name,building_name,s.type) /(24*30) AS AvgNumberOfTelemeteryRecordsPerHour
    FROM prd_db.transformed.site_core_sites s
    LEFT JOIN prd_db.transformed.customers cust
           ON (s.customer_id = cust.customer_id)
    LEFT JOIN central_monitoring_db.transformed.site_volume_by_month sv
           ON (s.id = sv.site_id )
QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_name,site_name,building_name,s.type ORDER BY month_start_date DESC) = 1
)
SELECT 
    customer_name,
    site_name,
    building_name,
    site_type,
    gross_area_from_site_core,
    gross_area_from_twin,
    rentable_area_from_twin, 
    SUM(NumberOfPointsPerBuilding) AS number_of_points_per_building,
    SUM(AvgNumberOfTelemeteryRecordsPerHour) AS avg_number_of_telemetery_records_per_hour
FROM cte_building_sum
GROUP BY     customer_name,
    site_name,
    building_name,
    site_type,
    gross_area_from_site_core,
    gross_area_from_twin,
    rentable_area_from_twin
;