-- ******************************************************************************************************************************
-- Create Sites view
-- ******************************************************************************************************************************
CREATE OR REPLACE VIEW transformed.sites_v AS
	SELECT
		dc.site_id,
		dc.portfolio_id,
		dc.customer_id,
		dc.name,
		dt.twin_id AS building_id,
		dt.name AS building_name,
		dt.raw_json_value:customProperties.type::STRING AS type,
        dt.raw_json_value,
		dt.model_id,
		dt.raw_json_value:customProperties.address:city::STRING AS address_city,
		dt.raw_json_value:customProperties.address:country::STRING AS address_country,
		dt.raw_json_value:customProperties.address:postalCode::STRING AS address_postal_code,
		dt.raw_json_value:customProperties.address:region::STRING AS address_region,
		dt.raw_json_value:coordinates:latitude::DECIMAL(8,6) AS coordinates_latitude,
		dt.raw_json_value:coordinates:longitude::DECIMAL(9,6) AS coordinates_longitude,
		dt.raw_json_value:customProperties.coordinates:latitude::DECIMAL(8,6) AS custom_latitude,
		dt.raw_json_value:customProperties.coordinates:longitude::DECIMAL(9,6) AS custom_longitude,
        COALESCE(coordinates_latitude,custom_latitude) AS latitude,
        COALESCE(coordinates_longitude,custom_longitude) AS longitude,
		dc.time_zone,
		CURRENT_REGION() AS region
	FROM transformed.directory_core_sites dc
		LEFT JOIN transformed.twins dt
			   ON (dc.site_id = dt.site_id)
              AND (dt.model_id IN ('dtmi:com:willowinc:Building;1','dtmi:com:willowinc:BuildingTower;1','dtmi:com:willowinc:Substructure;1','dtmi:com:willowinc:airport:AirportTerminal;1'))
	WHERE IFNULL(dt.is_deleted,false) = false
	QUALIFY ROW_NUMBER() OVER (PARTITION BY dc.site_id ORDER BY dc.site_id, model_id,dc._last_updated_at desc) = 1
    ;
	
CREATE OR REPLACE TABLE transformed.sites AS SELECT * FROM transformed.sites_v;


