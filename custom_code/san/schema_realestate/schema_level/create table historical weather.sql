create or replace TABLE TRANSFORMED.sites_weather_historical (
    station_id          VARCHAR(10),
    date                DATE,
    cdd                 NUMBER(12,1),
    hdd                 NUMBER(12,1),
    site_id             VARCHAR(36),
    site_name           VARCHAR(255),
    longitude           FLOAT,
    latitude            FLOAT,
    temperature_unit    VARCHAR(50),
    temperature_threshold NUMBER(12,1)
);