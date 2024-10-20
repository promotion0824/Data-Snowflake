drop view if exists published.UTILITY_VS_METERING_KPI;

call utils.create_table_from_stage('@raw.ADHOC_ESG/electric_bill/60MP_Investa_Utility_Data.csv', 'transformed.utility_data');

select * from  
-- update
transformed.utility_data
 set "consumption_kwh" = '224922.4'
where "consumption_kwh" =  '224922.4`';

select * from  
-- update
transformed.utility_data
SET "cost" = REPLACE("cost",'$','')
;
select * from  
-- update
transformed.utility_data
SET "cost" = REPLACE("cost",',','')
;

select * from published.connectivity_scores;
--select * from uat_db.published.connectivity_scores;

select * from published.utility_vs_metering_kpi;

CREATE VIEW published.utility_vs_metering_kpi AS
SELECT 
    "interval_start" AS interval_start,
    "interval_end" AS interval_end,
    AVG("consumption_kwh") AS utility_consumption_kwh,
    AVG("demand_kva") AS utility_demand_kva,
    AVG("cost") AS utility_cost,
    SUM(daily_usage_kwh) AS metered_consumption_kwh,
    ROUND(metered_consumption_kwh/utility_consumption_kwh,2) * 100 AS utility_vs_metering_kpi
FROM transformed.utility_data u
JOIN published.electrical_metering_detail m 
  ON m.date_local BETWEEN "interval_start" AND "interval_end"
WHERE 
        site_name ILIKE '60 Martin Place'
    AND date_local >= '2021-11-01' 
    AND date_local < '2022-11-01'
GROUP BY
"interval_start",
"interval_end"
ORDER BY "interval_start";

WITH cte_metering AS
SELECT left(date_local,7) AS Month, SUM(daily_usage_kwh), SUM(virtual_daily_usage_kwh)
FROM published.electrical_metering_detail
WHERE site_name ILIKE '60 Martin Place'
AND date_local >= '2021-11-01'
GROUP BY LEFT(date_local,7)
ORDER BY LEFT(date_local,7);


-- by asset
WITH cte_switchboard AS (
select left(date_local,7) AS Month,sum(daily_usage_kwh) as total_kwh, asset_name
from prd_db.published.electrical_metering_detail
where site_name ilike '60 Martin Place'
and date_local >= '2021-11-01'
group by left(date_local,7),asset_name
order by left(date_local,7),asset_name
)
    SELECT *
    FROM cte_switchboard
    PIVOT(SUM(total_kwh) FOR asset_name IN ('MSB-LGD-01', 'MSB-LGD-02')) 
     AS P(month, "MSB-LGD-01", "MSB-LGD-02")
ORDER BY month
;



-- daily total for 2021-11
select date_local,sum(daily_usage_kwh)
from prd_db.published.electrical_metering_detail
where site_name ilike '60 Martin Place'
and date_local between '2021-11-01' and '2021-11-30'
and 
group by date_local
order by date_local;