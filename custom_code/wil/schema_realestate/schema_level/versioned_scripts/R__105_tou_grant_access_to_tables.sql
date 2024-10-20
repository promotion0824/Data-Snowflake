-- ******************************************************************************************************************************
-- Create raw ontology tables
-- ******************************************************************************************************************************

GRANT SELECT ON raw.stage_tou_nyiso_LBMP TO ROLE weather_loading;
GRANT SELECT ON raw.stage_tou_nyiso_load_forecast TO ROLE weather_loading;
GRANT SELECT ON raw.stage_tou_hhng_spot TO ROLE weather_loading;
GRANT SELECT ON raw.stage_tou_nyharbor_nyiso_fo_spot TO ROLE weather_loading;
GRANT SELECT ON raw.stage_tou_rggi_auction TO ROLE weather_loading;
