CREATE OR REPLACE TABLE transformed.sites_weather_historical (
    station_id          VARCHAR(10),
    date                DATE,
    is_weekday          BOOLEAN,
    cdd                 NUMBER(12,1),
    hdd                 NUMBER(12,1),
    building_id         VARCHAR(255),
    building_name       VARCHAR(255),
    site_id             VARCHAR(36),
    site_name           VARCHAR(255),
    longitude           FLOAT,
    latitude            FLOAT,
    temperature_unit    VARCHAR(50),
    temperature_threshold NUMBER(12,1),
    _last_updated_at    TIMESTAMP_NTZ(9)
);