-- ******************************************************************************************************************************
-- Stored procedure that persists aggregate data
-- ******************************************************************************************************************************

CREATE OR REPLACE PROCEDURE prd_db.transformed.merge_site_volume_by_month_sp()
    RETURNS STRING
    LANGUAGE SQL
  AS
  $$
    MERGE INTO prd_db.transformed.site_volume_by_month AS tgt
		   USING (
          WITH watermark AS 
          (
            SELECT DISTINCT
              IFNULL(MAX(month_start_date),'2019-01-01') AS max_date 
            FROM prd_db.transformed.site_volume_by_month 
          )
          SELECT 
              DATE_TRUNC('MONTH', date_local) AS month_start_date,
              site_id,
              connector_id,
              approx_count_distinct(trend_id) AS count_of_telemetry_points,
              count(trend_id) AS count_of_rows
          FROM prd_db.transformed.telemetry
          WHERE month_start_date >= (SELECT DATEADD('d',-1,max_date) FROM watermark)
          GROUP BY 
              month_start_date,
              site_id,
              connector_id
		  ) AS src
			ON (tgt.month_start_date = src.month_start_date) AND (tgt.site_id = src.site_id) AND (tgt.connector_id = src.connector_id)
		  WHEN MATCHED THEN
			UPDATE 
			SET 
			  tgt.count_of_telemetry_points = src.count_of_telemetry_points,
			  tgt.count_of_rows = src.count_of_rows
			  --tgt._last_updated_at = SYSDATE()
		  WHEN NOT MATCHED THEN
			INSERT (
        month_start_date,
        site_id,
        connector_id,
        count_of_telemetry_points,
        count_of_rows
				)		
			VALUES (
				src.month_start_date,
        src.site_id,
        src.connector_id,
        src.count_of_telemetry_points,
        src.count_of_rows
      );
$$