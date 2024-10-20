-- ------------------------------------------------------------------------------------------------------------------------------
-- Create tables for historical weather data

-- ------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE transformed.sites_weather_historical (
    station_id          VARCHAR(10),
    date                DATE,
    cdd                 NUMBER(12,1),
    hdd                 NUMBER(12,1),
    site_id             VARCHAR(36),
    site_name           VARCHAR(255),
    longitude           FLOAT,
    latitude            FLOAT,
    temperature_unit    VARCHAR(50),
    temperature_threshold NUMBER(12,1),
    _last_updated_at    TIMESTAMP_NTZ(9)
);
CREATE OR REPLACE TABLE transformed.sites_hourly_temp_historical (
    station_id          VARCHAR(10),
    date_hour           TIMESTAMP,
    date                DATE,
    temperature         NUMBER(12,1),
    site_id             VARCHAR(36),
    site_name           VARCHAR(255),
    longitude           FLOAT,
    latitude            FLOAT,
    temperature_unit    VARCHAR(50),
    customer_id         VARCHAR(36),
    _last_updated_at    TIMESTAMP_NTZ(9)
);