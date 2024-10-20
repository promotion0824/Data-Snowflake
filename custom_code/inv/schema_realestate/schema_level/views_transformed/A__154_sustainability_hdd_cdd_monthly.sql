-- ******************************************************************************************************************************
-- Create view
-- ******************************************************************************************************************************

CREATE OR REPLACE VIEW transformed.sustainability_hdd_cdd_monthly AS

    SELECT
        DATE_TRUNC('MONTH',date) AS month_start_date,
        SUM(hdd) AS hdd,
        SUM(cdd) AS cdd,
        SUM(hdd) + SUM(cdd) as degree_days,
        temperature_unit,
        building_id,
        building_name
    FROM transformed.sites_weather_data ca
    GROUP BY
        month_start_date,
        temperature_unit,
        building_id,
        building_name
