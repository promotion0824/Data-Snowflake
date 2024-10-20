-- ------------------------------------------------------------------------------------------------------------------------------
-- Create sustainability_eui_by_tenant
-- ------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE VIEW published.sustainability_eui_by_tenant AS
WITH cte_tenant_unit AS (
SELECT
    tenant_name,
    asset_id AS tenant_account,
    MAX(tenant_unit_rentable_area) AS tenant_leased_area,
    building_id,
    building_name,
    site_id,
    site_name,
    MAX(building_rentable_area) AS building_rentable_area,
    DATE_TRUNC(month, date_local) AS month_start_date,
    SUM(power_consumption) AS energy_consumption
FROM transformed.tenant_energy_utility_account
GROUP BY month_start_date, tenant_name,tenant_account, building_id, building_name,site_id, site_name
)
SELECT 
    month_start_date,
    building_id,
    building_name,
    site_id,
    site_name,
    tenant_name,
    tenant_account,
    ROUND(SUM(energy_consumption),0) AS energy_consumption,
    ROUND(RATIO_TO_REPORT(SUM(energy_consumption)) OVER (PARTITION BY month_start_date, site_name),3) AS tenant_pct_total_consumption,
    SUM(tenant_leased_area) AS tenant_leased_area,
    ROUND(SUM(tenant_leased_area) / building_rentable_area ,3) AS tenant_pct_total_leased
FROM cte_tenant_unit
GROUP BY building_id, building_name,site_id, site_name, tenant_name, tenant_account, month_start_date, building_rentable_area
ORDER BY month_start_date DESC, energy_consumption DESC
;