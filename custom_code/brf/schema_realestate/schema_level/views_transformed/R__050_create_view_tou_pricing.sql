
-- auction dates spread out
CREATE OR REPLACE VIEW transformed.tou_rggi_auction_days_v AS
WITH cte_rggi AS (
        SELECT
                auction,
                date AS start_date,
                COALESCE(LEAD(date) OVER (ORDER BY date) - 1,SYSDATE()) as end_date
        FROM transformed.tou_rggi_auction
)
SELECT 
        a.auction,
        cte_rggi.start_date,
        cte_rggi.end_date,
        d.date_time_hour,
        a.quantity_offered,
        a.ccr_sold,
        a.quantity_sold,
        a.clearing_price,
        a.total_proceeds,
        a.file_name,
        a._ingested_at,
        a._last_updated_at
FROM transformed.date_hour d
LEFT JOIN cte_rggi ON d.date BETWEEN start_date AND end_date
LEFT JOIN transformed.tou_rggi_auction a ON cte_rggi.auction = a.auction AND cte_rggi.start_date = a.date
WHERE d.date_time_hour < SYSDATE() AND d.date_time_hour >= '2019-01-01'
;

CREATE OR REPLACE VIEW transformed.tou_nyharbor_nyiso_fo_spot_v AS
SELECT
        date AS start_date,
        COALESCE(lead(date) OVER (ORDER BY date),SYSDATE()) AS end_date,
        fo_spot_price,
        file_name,
        _ingested_at,
        _last_updated_at
FROM transformed.tou_nyharbor_nyiso_fo_spot
;

CREATE OR REPLACE VIEW transformed.tou_hhng_spot_v AS
SELECT
        DATEADD(hh,10,date) AS start_date,
        COALESCE(DATEADD(s,-1,DATEADD(hh,10,lead(date) OVER (ORDER BY date))) ,SYSDATE()) AS end_date,
        ng_spot_price,
        file_name,
        _ingested_at,
        _last_updated_at
FROM transformed.tou_hhng_spot
; 

CREATE OR REPLACE VIEW transformed.tou_nyiso_load_forecast_v AS
SELECT 
        time_stamp,
        capitl,
        centrl,
        dunwod,
        genese,
        hud_vl,
        longil,
        mhk_vl,
        millwd,
        nyc,
        north,
        west,
        nyiso,
        file_name,
        _ingested_at,
        _last_updated_at
FROM transformed.tou_nyiso_load_forecast;

CREATE OR REPLACE VIEW transformed.tou_nyiso_LBMP_v AS
SELECT 
        timestamp,
        name,
        ptid,
        lbmp_cost_per_mwhr,
        marginal_cost_losses_mwhr,
        marginal_cost_congestion_mwhr,
        file_name,
        _ingested_at,
        _last_updated_at
FROM transformed.tou_nyiso_LBMP
WHERE name = 'N.Y.C.'; 




CREATE OR REPLACE VIEW transformed.tou_pricing AS
WITH cte_constants AS (
    SELECT 
        3 AS VOM,
        .000288962 AS g_ue,
        0.137381 AS spot_price_fo_conversion,
        1102.31 AS us_ton_conversion_factor
)
    SELECT 
        d.date_time_hour,
        c.vom,
        lbmp.lbmp_cost_per_mwhr AS LBMP,
        rggi.clearing_price AS RA_n,
        fo.fo_spot_price AS Spot_Price_FO,
        ng.ng_spot_price AS Spot_Price_Nat_Gas,
        load.nyc AS HLF_i,
        fo.fo_spot_price / c.spot_price_fo_conversion AS SPOT_PRICE_FO_CONVERTED,
        c.g_ue,
        CASE WHEN SPOT_PRICE_FO_CONVERTED < Spot_Price_Nat_Gas THEN 0.00007421 ELSE 0.00005311 END AS MF_n,
        MF_n AS g_n,
        RA_n * g_n * us_ton_conversion_factor AS RE_n,
        CASE WHEN SPOT_PRICE_FO_CONVERTED < Spot_Price_Nat_Gas THEN SPOT_PRICE_FO_CONVERTED ELSE Spot_Price_Nat_Gas END AS MSP_n,
        (LBMP-VOM) / (RE_n + MSP_n) AS IHR_n_prelim, --if < 5 make it 0;  if > 17 make it 17;
        CASE WHEN IHR_n_prelim < 5 THEN 0 WHEN IHR_n_prelim > 17 THEN 17 ELSE IHR_n_prelim END AS IHR_n,
        IHR_n * MF_n AS HM_n,
        HM_n * HLF_i AS HMxHLF,
        SUM(HMxHLF) OVER (ORDER BY date ROWS BETWEEN 8759 PRECEDING AND CURRENT ROW) AS HMxHLF_rolling_sum,
        SUM(HLF_i) OVER (ORDER BY date ROWS BETWEEN 8759 PRECEDING AND CURRENT ROW) AS HLF_rolling_sum,
        HMxHLF_rolling_sum / HLF_rolling_sum AS RAM_n,
        HM_n - RAM_n AS HM_n_RAM_n,
        GREATEST(HM_n_RAM_n + g_ue,0) AS TOU_n
    FROM transformed.date_hour d
    CROSS JOIN cte_constants c
    LEFT JOIN transformed.tou_nyharbor_nyiso_fo_spot_v fo ON d.date >= fo.start_date AND d.date < fo.end_date
    LEFT JOIN transformed.tou_hhng_spot_v ng ON d.date_time_hour BETWEEN ng.start_date AND ng.end_date
    LEFT JOIN transformed.tou_nyiso_load_forecast_v load ON d.date_time_hour = load.time_stamp
    LEFT JOIN transformed.tou_nyiso_LBMP_v lbmp ON d.date_time_hour = lbmp.timestamp
    LEFT JOIN transformed.tou_rggi_auction_days_v rggi ON d.date = rggi.date_time_hour
    WHERE d.date_time_hour < SYSDATE()
      AND (lbmp.lbmp_cost_per_mwhr is not null or rggi.clearing_price is not null or fo.fo_spot_price is not null or ng.ng_spot_price or load.nyc is not null)
ORDER BY d.date_time_hour DESC
;