CREATE OR REPLACE PROCEDURE utils.initialize_site_daily_energy_thresholds_sp(date_from DATE, date_to DATE)
    RETURNS STRING
    LANGUAGE JAVASCRIPT
    STRICT
    EXECUTE AS OWNER
  AS
    $$
        let taskName = 'INITIAL_LOAD';
        let startDate = DATE_FROM;
        let endDate = DATE_TO;
        
        let dateFrom;
        let dateTo;
        
        let result_array = [];

        do {
            // Calculate first and last day of month
            dateFrom = new Date(startDate.getFullYear(), startDate.getMonth(), 1);
            dateTo = new Date(dateFrom.getFullYear(), dateFrom.getMonth() + 1, 0);
            
            
            result_array.push(`{"from":"${dateFrom.toISOString().split('T')[0]}", "to":"${dateTo.toISOString().split('T')[0]}""}`);

            snowflake.execute (
                {
                    sqlText: 'CALL calculate_site_daily_energy_thresholds_sp(:1, :2, :3)', 
                    binds: [dateFrom.toISOString().split('T')[0], dateTo.toISOString().split('T')[0], taskName]}
                );

            startDate.setDate(dateTo.getDate() + 1);
        }
        // Iterate for every month in the date range
        while (dateTo < endDate);

        return result_array;
    $$
;