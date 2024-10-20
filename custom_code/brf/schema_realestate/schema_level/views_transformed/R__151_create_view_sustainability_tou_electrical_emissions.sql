-- ******************************************************************************************************************************
-- Create view 
-- ******************************************************************************************************************************
CREATE OR REPLACE VIEW transformed.sustainability_tou_electrical_emissions_v AS
WITH cte_hours_in_month AS (
    SELECT DATE_TRUNC('MONTH',d.date) AS month,count(*) AS num_hours
    FROM transformed.date_hour d
    GROUP BY month

)
,cte_detail AS (
SELECT 
    ts.capability_id,
    ts.building_id,
    ts.trend_id,
    ts.site_id,
    ts.date_local,
    ts.date_time_local_hour,
    ts.hourly_usage,
    ts.unit,
    hourly_usage * tou_n * 1000 AS ghg_emissions_toun, 
    hourly_usage * g_ue * 1000 AS ghg_emissions,
    last_refreshed_at_utc
FROM transformed.agg_electrical_metering_hourly ts
JOIN transformed.tou_pricing tou ON ts.date_time_local_hour = tou.date_time_hour
WHERE 
      ts.sensor_type = 'Energy'
  AND ts.hourly_usage > 0
)
SELECT 
    d.building_id,
    d.trend_id,
    DATE_TRUNC('MONTH',date_local) AS month_start_date,
    SUM(hourly_usage) AS energy_consumption,
    d.unit,
    CASE WHEN d.unit ilike 'kWh' THEN energy_consumption * 3412.14 ELSE NULL END AS energy_consumption_btu,
    SUM(ghg_emissions_toun) AS ghg_emissions_tou,
    SUM(ghg_emissions) AS ghg_emissions_flat,
    ghg_emissions_tou / energy_consumption as emissions_factor_tou,
    ghg_emissions_flat / energy_consumption as emissions_factor_flat,
    'kgco2e/kwh' as emissions_factor_unit,
    'NYC Local Law 97' as emissions_factor_source,
    'electricity' AS energy_source,
    'electricity' AS service_type,
    'Scope 2' AS scope,
    COUNT(DISTINCT date_time_local_hour) AS hours_reporting,
    hours_reporting / MAX(num_hours) AS pct_of_hours_reporting,
    a.capability_id,
    a.capability_name,
    a.model_id,
    a.asset_id,
    a.asset_name,
    a.building_name,
    a.time_zone,
    a.customer_id,
    a.portfolio_id,
    a.building_type,
    a.building_gross_area,
    a.building_gross_area_unit,
    a.building_rentable_area,
    MAX(CONVERT_TIMEZONE( 'UTC',a.time_zone, last_refreshed_at_utc)) AS last_refreshed_at_local
FROM cte_detail d
JOIN cte_hours_in_month ON month_start_date = month
JOIN transformed.electrical_metering_assets a ON d.trend_id = a.trend_id
--LEFT JOIN transformed.buildings b ON a.building_id = b.building_id
WHERE (top_level_model_id IN ('dtmi:com:willowinc:Switchboard;1','dtmi:com:willowinc:Switchgear;1')
OR  model_id_asset IN ('dtmi:com:willowinc:ElectricalMeter;1'))
GROUP BY 
    d.building_id,
    month_start_date,
    d.unit,
    d.trend_id,
    a.capability_id,
    a.capability_name,
    a.model_id,
    a.asset_id,
    a.asset_name,
    a.building_name,
    a.time_zone,
    a.customer_id,
    a.portfolio_id,
    a.building_type,
    a.building_gross_area,
    a.building_gross_area_unit,
    a.building_rentable_area
;

CREATE OR REPLACE TABLE transformed.sustainability_tou_electrical_emissions AS 
    SELECT * FROM transformed.sustainability_tou_electrical_emissions_v;
