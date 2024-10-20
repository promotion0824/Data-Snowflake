-- ******************************************************************************************************************************
-- Stored procedure that persists aggregate data
-- ******************************************************************************************************************************

CREATE OR REPLACE PROCEDURE central_monitoring_db.transformed.merge_site_volume_by_month_sp()
    RETURNS STRING
    LANGUAGE SQL
  AS
  $$
  BEGIN

    CREATE OR REPLACE TEMPORARY TABLE raw.temp_site AS 
    SELECT * FROM central_monitoring_db.raw.site_volume_by_month_str
    QUALIFY ROW_NUMBER() OVER (PARTITION BY month_start_date, site_id, connector_id, model_id  ORDER BY _last_updated DESC) = 1;
    
    DELETE FROM central_monitoring_db.transformed.site_volume_by_month svm
    USING raw.temp_site 
    WHERE svm.month_start_date = temp_site.month_start_date
      AND svm.site_id = temp_site.site_id;

    INSERT INTO central_monitoring_db.transformed.site_volume_by_month ( 
        month_start_date,
        site_id,
        site_name,
        connector_id,
        count_of_telemetry_points,
        count_of_rows,
        building_id,
        building_name,
        type,
        gross_area,
        rentable_area,
        model_id,
        customer_abbreviation,
        snowflake_created_date
  ) 
      SELECT 
        month_start_date,
        site_id,
        site_name,
        connector_id,
        count_of_telemetry_points,
        count_of_rows,
        building_id,
        building_name,
        type,
        gross_area,
        rentable_area,
        model_id,
        customer_abbreviation,
        snowflake_created_date
      FROM raw.temp_site;
END;
$$
;