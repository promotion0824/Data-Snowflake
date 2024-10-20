-- ------------------------------------------------------------------------------------------------------------------------------
-- Create procedure to get the connectivity scores for any model(s)
-- ------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION  transformed.connectivity_scores_by_model(AssetsModelsList ARRAY, CapabilitiesModelsList ARRAY)
    RETURNS TABLE(
        date DATE,
        date_time_hour TIMESTAMP_NTZ,
        asset_id VARCHAR(255),
        asset_name VARCHAR(255),
        model_id_asset VARCHAR(255),
        trend_count_expected NUMBER(31,0),
        trend_count_actual NUMBER(31,0),
        capability_models ARRAY,
        num_sensors NUMBER(18,0)
    )
  AS
    $$
         WITH cte_telemetry AS (
                SELECT 
                    asset_id,
                    date_time_local_15min,
                    date_time_local_hour,
                    date_local,
                    COUNT(DISTINCT trend_id) AS trend_count
                FROM published.telemetry_twins_rolling_7_days telemetry
                WHERE telemetry.trend_id IN (SELECT trend_id
                                            FROM transformed.capabilities_assets
                                                    WHERE model_id_asset in (
                                                                SELECT value
                                                                FROM LATERAL FLATTEN(INPUT => AssetsModelsList) 
                                                            )
                                                    AND model_id in (
                                                                SELECT value
                                                                FROM LATERAL FLATTEN(INPUT => CapabilitiesModelsList) 
                                                            )
                                        )

                GROUP BY 
                    asset_id,
                    date_time_local_15min,
                    date_time_local_hour,
                    date_local
                )
                , cte_assets AS (
                SELECT 
                    asset_id,
                    asset_name,
                    model_id_asset,
                    COUNT(*) AS sensors,
                    ARRAY_AGG(model_id) WITHIN GROUP (ORDER BY model_id ASC) AS capability_models
                FROM transformed.capabilities_assets
                                                    WHERE model_id_asset in (
                                                                SELECT value
                                                                FROM LATERAL FLATTEN(INPUT => AssetsModelsList) 
                                                            )
                                                    AND model_id in (
                                                                SELECT value
                                                                FROM LATERAL FLATTEN(INPUT => CapabilitiesModelsList) 
                                                            )
                GROUP BY 
                    asset_id,
                    asset_name,
                    model_id_asset
                )
                SELECT
                    dh.date,
                    dh.date_time_hour,
                    cte_assets.asset_id,
                    cte_assets.asset_name,
                    cte_assets.model_id_asset,
                    MAX(cte_assets.sensors) * 4 AS trend_count_expected,
                    SUM(cte_telemetry.trend_count) AS trend_count_actual,
                    cte_assets.capability_models,
                    MAX(cte_assets.sensors) AS num_sensors
                FROM published.date_hour dh
                CROSS JOIN cte_assets
                LEFT JOIN cte_telemetry 
                ON (dh.date_time_hour = cte_telemetry.date_time_local_hour)
                AND (cte_telemetry.asset_id = cte_assets.asset_id)
                -- Ensure we get at least 7 days of data, but the min(date) leaves this the option to use an extended time range in the future
                WHERE dh.date >= (SELECT LEAST(IFNULL(MIN(date_local),DATEADD(DAY,-7,CURRENT_DATE)),DATEADD(DAY,-7,CURRENT_DATE)) FROM cte_telemetry)
                AND dh.date <= (SELECT MAX(date_local) FROM cte_telemetry)
                GROUP BY 
                    dh.date,
                    dh.date_time_hour,
                    cte_assets.asset_id,
                    cte_assets.asset_name,
                    cte_assets.model_id_asset,
                    cte_assets.capability_models
                ORDER BY 
                    cte_assets.asset_id,
                    cte_assets.asset_name,
                    cte_assets.model_id_asset,
                    dh.date_time_hour

    $$
;