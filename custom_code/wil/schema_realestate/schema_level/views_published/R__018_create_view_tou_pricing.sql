
CREATE OR REPLACE VIEW published.tou_rggi_auction AS
SELECT 
        auction,
        date AS start_date,
        COALESCE(LEAD(date) OVER (ORDER BY date) - 1,SYSDATE()) as end_date,
        quantity_offered,
        ccr_sold,
        quantity_sold,
        clearing_price,
        total_proceeds,
        file_name,
        _ingested_at,
        _last_updated_at
FROM transformed.tou_rggi_auction
;

-- auction dates spread out
CREATE OR REPLACE VIEW published.tou_rggi_auction_days AS
SELECT 
        auction,
        rggi.start_date,
        rggi.end_date,
        d.date_time_hour,
        quantity_offered,
        ccr_sold,
        quantity_sold,
        clearing_price,
        total_proceeds,
        file_name,
        _ingested_at,
        _last_updated_at
FROM transformed.date_hour d
LEFT JOIN published.tou_rggi_auction rggi ON d.date BETWEEN start_date AND end_date
;

CREATE OR REPLACE VIEW published.tou_nyharbor_nyiso_fo_spot AS
SELECT
        date AS start_date,
        COALESCE(lead(date) OVER (ORDER BY date),SYSDATE()) AS end_date,
        fo_spot_price,
        file_name,
        _ingested_at,
        _last_updated_at
FROM transformed.tou_nyharbor_nyiso_fo_spot
;

CREATE OR REPLACE VIEW published.tou_hhng_spot AS
SELECT
        DATEADD(hh,10,date) AS start_date,
        COALESCE(DATEADD(s,-1,DATEADD(hh,10,lead(date) OVER (ORDER BY date))) ,SYSDATE()) AS end_date,
        ng_spot_price,
        file_name,
        _ingested_at,
        _last_updated_at
FROM transformed.tou_hhng_spot
; 

CREATE OR REPLACE VIEW published.tou_nyiso_load_forecast AS
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

CREATE OR REPLACE VIEW published.tou_nyiso_LBMP AS
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




CREATE OR REPLACE VIEW published.tou_pricing AS
SELECT 
d.date_time_hour, lbmp.lbmp_cost_per_mwhr, rggi.clearing_price AS rggi, fo.fo_spot_price AS Spot_Price_FO, ng.ng_spot_price AS Spot_Price_Nat_Gas,load.nyc AS NYC_Load_Forecast
FROM transformed.date_hour d
LEFT JOIN published.tou_nyharbor_nyiso_fo_spot fo ON d.date >= fo.start_date AND d.date < fo.end_date
LEFT JOIN published.tou_hhng_spot ng ON d.date_time_hour BETWEEN ng.start_date AND ng.end_date
LEFT JOIN published.tou_nyiso_load_forecast load ON d.date_time_hour = load.time_stamp
LEFT JOIN published.tou_nyiso_LBMP lbmp ON d.date_time_hour = lbmp.timestamp
LEFT JOIN published.tou_rggi_auction_days rggi ON d.date = rggi.date_time_hour
WHERE d.date_time_hour < SYSDATE() 
and  (lbmp.lbmp_cost_per_mwhr is not null or rggi.clearing_price is not null or fo.fo_spot_price is not null or ng.ng_spot_price or load.nyc is not null)
ORDER BY d.date_time_hour DESC
;