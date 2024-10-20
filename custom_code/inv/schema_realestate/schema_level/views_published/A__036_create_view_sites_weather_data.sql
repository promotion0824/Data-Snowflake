-- ------------------------------------------------------------------------------------------------------------------------------
-- Create View - sourced from weatherBit
-- ------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW published.sites_weather_data AS

SELECT DISTINCT
    date,
    is_weekday,
    hdd,
    cdd,
    temperature_unit,
    site_id,
    site_name,
    building_id,
    building_name
FROM transformed.sites_weather_data
;