-- ******************************************************************************************************************************
-- Stored procedure that populates site_defaults for newly ingested sites
-- ******************************************************************************************************************************

CREATE OR REPLACE PROCEDURE transformed.insert_site_defaults_sp(task_name VARCHAR)
    RETURNS STRING
    LANGUAGE SQL
  AS
    $$
      BEGIN
        -- TODO: Site default temp setpoint should come from ADT as 'dtmi:com:willowinc:ZoneAirTemperatureSetpointDefault;1'
        INSERT INTO transformed.site_defaults (site_id, type, default_value, _is_active, _valid_from, _valid_to, _created_by_task, _last_updated_by_task)
          SELECT 
            sites.site_id, 
            'ZoneAirTemperatureSetpointDefault',
            PARSE_JSON(' { "value": 22.5, "unit": "degC" } '),
            true,
            TO_TIMESTAMP('0000-01-01'), 
            TO_TIMESTAMP('9999-12-31'),
            :task_name,
            :task_name
          FROM transformed.directory_core_sites sites
            LEFT JOIN transformed.site_defaults defaults
              ON (sites.site_id = defaults.site_id AND defaults.type = 'ZoneAirTemperatureSetpointDefault')
          WHERE defaults.site_id IS NULL;

        -- TODO: Site default working hours should come from ADT as 'dtmi:com:willowinc:WorkingHoursDefault;1'
        INSERT INTO transformed.site_defaults (site_id, type, default_value, _is_active, _valid_from, _valid_to, _created_by_task, _last_updated_by_task)
          SELECT 
            sites.site_id, 
            'WorkingHours',
            PARSE_JSON(' { "hourStart": 8, "hourEnd": 18 } '),
            true,
            TO_TIMESTAMP('0000-01-01'), 
            TO_TIMESTAMP('9999-12-31'),
            :task_name,
            :task_name
          FROM transformed.directory_core_sites sites
            LEFT JOIN transformed.site_defaults defaults
              ON (sites.site_id = defaults.site_id AND defaults.type = 'WorkingHours')
          WHERE defaults.site_id IS NULL;    
      END;
    $$
;