CREATE OR REPLACE PROCEDURE utils.calculate_site_daily_electrical_energy_usage_sp(date_from DATE, date_to DATE, task_name STRING)
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
      MERGE INTO transformed.site_daily_electrical_energy_usage AS tgt 
        USING (
          
          SELECT 
            site_id,
            date,
            SUM(last_daily_measurement_value_kwh) AS last_daily_measurement_value_kwh,
            SUM(daily_usage_kwh) AS daily_usage_kwh,
            SYSDATE() AS _created_at,
            SYSDATE() AS _last_updated_at
          FROM transformed.daily_usage_per_total_elec_energy_sensor
          WHERE date BETWEEN :1 AND :2
          GROUP BY 
            site_id,
            date
          
        ) AS src
          ON (tgt.site_id = src.site_id AND tgt.date = src.date)
        WHEN MATCHED THEN
          UPDATE 
          SET 
            tgt.last_daily_measurement_value_kwh = src.last_daily_measurement_value_kwh,
            tgt.daily_usage_kwh = src.daily_usage_kwh,
            tgt._last_updated_at = SYSDATE(),
            tgt._last_updated_by_task = :3
        WHEN NOT MATCHED THEN
          INSERT (
            site_id, 
            date, 
            last_daily_measurement_value_kwh, 
            daily_usage_kwh,
            _created_at,
            _created_by_task,
            _last_updated_at,
            _last_updated_by_task) 
          VALUES (
            src.site_id, 
            src.date,
            src.last_daily_measurement_value_kwh, 
            src.daily_usage_kwh, 
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
