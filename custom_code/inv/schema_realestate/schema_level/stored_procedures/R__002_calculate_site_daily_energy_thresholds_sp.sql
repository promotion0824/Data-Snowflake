CREATE OR REPLACE PROCEDURE utils.calculate_site_daily_energy_thresholds_sp(date_from DATE, date_to DATE, task_name STRING)
    RETURNS STRING
    LANGUAGE JAVASCRIPT
    STRICT
    EXECUTE AS OWNER
  AS
    $$
    var dateFrom = DATE_FROM.toISOString();
    var dateTo = DATE_TO.toISOString();
    var taskName = (TASK_NAME == null) ? 'unknown' : TASK_NAME;

    var sqlCommand = `
      MERGE INTO transformed.site_daily_energy_thresholds AS tgt 
        USING (
          WITH cte_site_initial_values AS (

            SELECT 
              sites.site_id, 
              ZEROIFNULL(site_usage.last_daily_measurement_value_kwh) AS initial_value_kwh
            FROM transformed.sites sites
              LEFT JOIN transformed.site_daily_electrical_energy_usage site_usage
                ON (sites.site_id = site_usage.site_id AND site_usage.date = DATEADD(DAY, -1, CAST(:1 AS DATE)))

          ), cte_days AS (

            SELECT 
              date,
              is_weekday,
              DAY(CAST(:2 AS DATE)) AS nr_days_in_month,
              SUM(CASE WHEN is_weekday THEN 0 ELSE 1 END) OVER (PARTITION BY (MONTH(date))) AS nr_of_non_workdays
            FROM utils.dates
            WHERE date BETWEEN :1 AND :2

          ), cte_site_daily_thresholds AS (

            SELECT 
              d.date,
              d.is_weekday,
              v.site_id,
              v.initial_value_kwh,
              t_low.threshold_value / ((nr_days_in_month - nr_of_non_workdays) + nr_of_non_workdays * 0.6) * (CASE WHEN is_weekday THEN 1 ELSE 0.6 END) AS daily_low_threshold_kwh,
              t_high.threshold_value / ((nr_days_in_month - nr_of_non_workdays)  + nr_of_non_workdays * 0.6) * (CASE WHEN is_weekday THEN 1 ELSE 0.6 END) AS daily_high_threshold_kwh
            FROM cte_site_initial_values v
              CROSS JOIN cte_days d
              JOIN transformed.site_thresholds t_low 
                ON (v.site_id = t_low.site_id AND t_low.type = 'EnergyScore_Low' AND d.date BETWEEN t_low._valid_from AND t_low._valid_to)
              JOIN transformed.site_thresholds t_high 
                ON (v.site_id = t_high.site_id AND t_high.type = 'EnergyScore_High' AND d.date BETWEEN t_high._valid_from AND t_high._valid_to)

          )

          SELECT 
            site_id,
            date,
            is_weekday,
            initial_value_kwh,
            daily_low_threshold_kwh,
            daily_high_threshold_kwh,
            SUM(daily_low_threshold_kwh) OVER (PARTITION BY site_id ORDER BY date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS monthly_low_threshold_cum_sum_kwh,
            SUM(daily_high_threshold_kwh) OVER (PARTITION BY site_id ORDER BY date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS monthly_high_threshold_cum_sum_kwh,
            initial_value_kwh + monthly_low_threshold_cum_sum_kwh AS total_low_threshold_cum_sum_kwh,
            initial_value_kwh + monthly_high_threshold_cum_sum_kwh AS total_high_threshold_cum_sum_kwh,
            SYSDATE() AS _created_at,
            SYSDATE() AS _last_updated_at            
          FROM cte_site_daily_thresholds
          
        ) AS src
          ON (tgt.site_id = src.site_id AND tgt.date = src.date)
        WHEN MATCHED THEN
          UPDATE 
          SET
            tgt.is_weekday = src.is_weekday,
            tgt.initial_value_kwh = src.initial_value_kwh,
            tgt.daily_low_threshold_kwh = src.daily_low_threshold_kwh,
            tgt.daily_high_threshold_kwh = src.daily_high_threshold_kwh,
            tgt.monthly_low_threshold_cum_sum_kwh = src.monthly_low_threshold_cum_sum_kwh,
            tgt.monthly_high_threshold_cum_sum_kwh = src.monthly_high_threshold_cum_sum_kwh,
            tgt.total_low_threshold_cum_sum_kwh = src.total_low_threshold_cum_sum_kwh,
            tgt.total_high_threshold_cum_sum_kwh = src.total_high_threshold_cum_sum_kwh,
            tgt._last_updated_at = SYSDATE(),
            tgt._last_updated_by_task = :3
        WHEN NOT MATCHED THEN
          INSERT (
            site_id, 
            date, 
            is_weekday,
            daily_low_threshold_kwh, 
            daily_high_threshold_kwh,
            monthly_low_threshold_cum_sum_kwh, 
            monthly_high_threshold_cum_sum_kwh,
            total_low_threshold_cum_sum_kwh, 
            total_high_threshold_cum_sum_kwh,
            _created_at,
            _created_by_task,
            _last_updated_at,
            _last_updated_by_task) 
          VALUES (
            src.site_id, 
            src.date,
            src.is_weekday,
            src.daily_low_threshold_kwh, 
            src.daily_high_threshold_kwh, 
            src.monthly_low_threshold_cum_sum_kwh,
            src.monthly_high_threshold_cum_sum_kwh, 
            src.total_low_threshold_cum_sum_kwh,
            src.total_high_threshold_cum_sum_kwh,
            SYSDATE(), 
            :3,
            SYSDATE(),
            :3
          )    
     `;
     
    try {
        snowflake.execute (
            {
                sqlText: sqlCommand, 
                binds: [dateFrom, dateTo, taskName]}
            );
        return 'Succeeded.'; 
        }
    catch (err)  {
        return "Failed: " + err;
        throw err;
        }
    $$
;