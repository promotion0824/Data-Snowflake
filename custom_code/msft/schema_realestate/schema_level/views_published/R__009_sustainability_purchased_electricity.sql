CREATE OR REPLACE VIEW sustainability_db.published.sustainability_electricity AS
SELECT
    'Purchased Electricity' AS "Name",
    'Contoso USA' AS "Organizational unit",
    'Torre Universal' AS "Facility",
     asset_name AS "Meter number",
     MIN(date_local) AS "Consumption start date",
     MAX(date_local) AS "Consumption end date",
     'Estimated' AS "Data Quality Type",  
     SUM(IFNULL(daily_usage_kwh,0)) AS "Quantity",
    'kwh' AS "Quantity Unit",
     CURRENT_TIMESTAMP AS "Transaction date",
    'Electricity' AS "Energy Type",
    'MS-TU electricity usage based from ActiveElectricalEnergySensor' AS "Description",
    'N/A' AS "Contractual instrument type",
    --'999' AS "Origin correlation ID",
    'CNFL' AS "Energy Provider Name"
FROM uat_db.published.electrical_metering_summary
WHERE date_local >= '2021-07-01'
GROUP BY
    LEFT(date_local,7),
    asset_name
ORDER by "Consumption end date"
;