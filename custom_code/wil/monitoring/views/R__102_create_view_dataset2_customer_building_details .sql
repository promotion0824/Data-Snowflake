-- dataset1_azure_consumption_by_customer

CREATE OR REPLACE VIEW central_monitoring_db.published.dataset2_customer_building_details AS

SELECT 
 cl.name AS customer_name
,customer_abbreviation
,site_name
,MAX(building_name) AS building_name
,MAX(type) AS type
,MAX(gross_area) AS gross_area
,MAX(rentable_area) AS rentable_area
FROM central_monitoring_db.published.site_volume_by_month sv
LEFT JOIN azure_consumption.costs.customer_lookup cl ON customer_abbreviation = UPPER(cl.abbreviation)
GROUP BY
 cl.name
,customer_abbreviation
,site_name