/* 
 ------------------------------------------------------------------------------------------------------------------------------
  This UDTF is used for the KPI Operations Dashboards
 ------------------------------------------------------------------------------------------------------------------------------
*/

--Drop the old function before group_by was added as an optional parameter
DROP FUNCTION IF EXISTS app_dashboards.get_building_data_udtf(VARCHAR,VARCHAR,DATE,DATE,BOOLEAN,BOOLEAN);

CREATE OR REPLACE FUNCTION app_dashboards.get_building_data_udtf(
    p_portfolio_id VARCHAR,
    p_sites VARCHAR,
    p_date_from DATE,
    p_date_to DATE,
    p_is_weekday BOOLEAN,
    p_is_business_hours BOOLEAN,
    group_by VARCHAR DEFAULT ''
    )
    RETURNS TABLE (portfolio_id VARCHAR, site_id VARCHAR, data_point_name VARCHAR, x_data DATE, y_data NUMERIC(3,2), y_data_uom VARCHAR)
    AS
    $$        
      WITH cte_site_filter AS (

          SELECT s.site_id
          FROM transformed.sites s
              LEFT JOIN TABLE(SPLIT_TO_TABLE(REPLACE(p_sites, ' ', ''), ',')) AS f
                  ON (COLLATE(s.site_id, 'en-ci') = f.value)  
          WHERE (COLLATE(s.portfolio_id, 'en-ci') = p_portfolio_id OR IFNULL(p_portfolio_id, '') = '')
              AND (f.value IS NOT NULL OR IFNULL(p_sites,'') = '')

      ), cte_scores_detail AS (
      
          SELECT 
            p_portfolio_id AS portfolio_id,
            scores.site_id,
            CAST(CASE WHEN UPPER(group_by) = 'DATE' THEN date ELSE NULL END AS DATE) AS x_data,
            SUM(comfort_score) / COUNT(comfort_score) AS comfort_score,
            SUM(energy_score)  / COUNT(energy_score) AS energy_score,
            SUM(overall_score) / COUNT(overall_score) AS overall_score
          FROM published.site_daily_operations_scores scores
            JOIN cte_site_filter site_filter
              ON (scores.site_id = site_filter.site_id)
          WHERE date BETWEEN p_date_from AND p_date_to
            AND (scores.is_weekday = p_is_weekday
             OR TO_BOOLEAN(p_is_weekday) IS NULL) 
            AND (scores.is_working_hour = p_is_business_hours
             OR TO_BOOLEAN(p_is_business_hours) IS NULL)             
            AND IFNULL(excludedFromComfortAnalytics, FALSE) = FALSE
          GROUP BY ALL
          
      ), cte_overall_score AS (
      
        SELECT
          portfolio_id,
          site_id,
          x_data,
          CAST(overall_score * 0.01 AS NUMERIC(3,2)) AS overall_score,
          CAST(comfort_score * 0.01 AS NUMERIC(3,2)) AS comfort_score,
          CAST(energy_score  * 0.01 AS NUMERIC(3,2)) AS energy_score
        FROM cte_scores_detail
        
      ), cte_unpivoted AS (
      
          SELECT * 
          FROM cte_overall_score
          UNPIVOT(kpi_value FOR kpi_name IN (comfort_score, energy_score, overall_score))
      )
      SELECT 
        portfolio_id,
        site_id,
        CASE kpi_name
          WHEN 'COMFORT_SCORE' THEN 'ComfortScore_LastValue'
          WHEN 'ENERGY_SCORE' THEN 'EnergyScore_LastValue'
          WHEN 'OVERALL_SCORE' THEN 'OperationsScore_LastValue'
        END AS data_point_name,
        x_data,
        kpi_value AS y_data,
        '%' AS y_data_uom
      FROM cte_unpivoted
      ORDER BY 1,2,3,4,5

    $$
    ;