-- ------------------------------------------------------------------------------------------------------------------------------
-- Thresholds for comfort score calculation
-- ------------------------------------------------------------------------------------------------------------------------------
INSERT INTO transformed.site_thresholds (site_id, type, threshold_value, _is_active, _valid_from, _valid_to)
  WITH cte_temp_thresholds AS (
    SELECT 'ZoneAirTemperatureDegC_Low' AS type, 21 AS threshold_value UNION ALL
    SELECT 'ZoneAirTemperatureDegC_High' AS type, 23 AS threshold_value
  )
  SELECT 
    s.site_id, 
    t.type,
    t.threshold_value,
    true,
    TO_TIMESTAMP('2000-01-01'), 
    TO_TIMESTAMP('9999-12-31')
  FROM transformed.directory_core_sites s
    CROSS JOIN cte_temp_thresholds t;
    
INSERT INTO transformed.site_thresholds (site_id, type, threshold_value, _is_active, _valid_from, _valid_to)
  WITH cte_temp_thresholds AS (
    SELECT 'EnergyScore_Low' AS type, 0 AS threshold_value UNION ALL
    SELECT 'EnergyScore_High' AS type, 0 AS threshold_value
  )
  SELECT 
    s.site_id, 
    t.type,
    t.threshold_value,
    true,
    TO_TIMESTAMP('2000-01-01'), 
    TO_TIMESTAMP('9999-12-31')
  FROM transformed.directory_core_sites s
    CROSS JOIN cte_temp_thresholds t;

-- ------------------------------------------------------------------------------------------------------------------------------
-- Thresholds for energy score calculation
-- ------------------------------------------------------------------------------------------------------------------------------

-- DEV values
{% if environment == 'dev' %}

    UPDATE transformed.site_thresholds
    SET threshold_value = 360000, _last_modified_at = SYSDATE()
    WHERE site_id = '404bd33c-a697-4027-b6a6-677e30a53d07' AND type = 'EnergyScore_Low';

    UPDATE transformed.site_thresholds
    SET threshold_value = 390000, _last_modified_at = SYSDATE()
    WHERE site_id = '404bd33c-a697-4027-b6a6-677e30a53d07' AND type = 'EnergyScore_High';

    UPDATE transformed.site_thresholds
    SET threshold_value = 60000, _last_modified_at = SYSDATE()
    WHERE site_id = '934638e3-4bd7-4749-bd52-bd6e47d0fbb2' AND type = 'EnergyScore_Low';

    UPDATE transformed.site_thresholds
    SET threshold_value = 80000, _last_modified_at = SYSDATE()
    WHERE site_id = '934638e3-4bd7-4749-bd52-bd6e47d0fbb2' AND type = 'EnergyScore_High';

{% endif %}

SELECT 1;