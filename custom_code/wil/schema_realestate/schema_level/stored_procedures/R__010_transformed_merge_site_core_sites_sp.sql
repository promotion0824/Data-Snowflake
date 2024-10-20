-- ******************************************************************************************************************************
-- Stored procedure that merges into insights
-- This is called via transformed.merge_insights_tk which is scheduled by CRON
-- USAGE:  CALL transformed.merge_insights_sp(SYSTEM$CURRENT_USER_TASK_NAME());
-- ******************************************************************************************************************************

CREATE OR REPLACE PROCEDURE transformed.merge_site_core_sites_sp(task_name VARCHAR)
    RETURNS STRING
    LANGUAGE SQL
  AS
    $$
  MERGE INTO transformed.site_core_sites AS tgt 
  USING (
    SELECT
      json_value:Id::STRING AS id,
      json_value:CustomerId::STRING AS customer_id,
      json_value:PortfolioId::STRING AS portfolio_id,	  
      json_value:Name::STRING AS name,
      json_value:Code::STRING AS code,
	  json_value:Address::STRING AS address,
	  json_value:State::STRING AS state,
	  json_value:Postcode::STRING AS postal_code,
	  json_value:Country::STRING AS country,
	  json_value:NumberOfFloors::INTEGER AS number_of_floors,
	  json_value:Area::STRING AS area,
	  json_value:LogoId::STRING AS logo_id,
	  json_value:Latitude::NUMBER(9,6) AS latitude,
	  json_value:Longitude::NUMBER(9,6) AS longitude,
	  json_value:TimezoneId::STRING AS time_zone_id,
	  json_value:Status::INTEGER AS status,
	  json_value:Suburb::STRING AS suburb,
	  json_value:Type::INTEGER AS type,
	  json_value:ConstructionYear::INTEGER AS construction_year,
	  json_value:SiteCode::STRING AS site_code,
	  json_value:CreatedDate::DATE AS source_created_date,
	  json_value:server::STRING AS server,
	  TRY_PARSE_JSON(json_value::VARIANT)::variant AS raw_json_value,
      true AS is_active,
      _stage_record_id,
      json_value:_loader_run_Id::STRING AS _loader_run_id,
      json_value:_ingested_at::TIMESTAMP_NTZ AS _ingested_at,
      _staged_at
    FROM raw.json_site_core_sites_str
    -- Make sure that the joining key is unique (take just the latest batch if there is more)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _ingested_at DESC) = 1
  ) AS src
    ON (tgt.id = src.id)
  WHEN MATCHED THEN
    UPDATE 
    SET 
		tgt.customer_id = src.customer_id,
		tgt.portfolio_id = src.portfolio_id,
		tgt.name = src.name,
		tgt.code = src.code,
		tgt.address = src.address,
		tgt.state = src.state,
		tgt.postal_code = src.postal_code,
		tgt.country = src.country,
		tgt.number_of_floors = src.number_of_floors,
		tgt.area = src.area,
		tgt.logo_id = src.logo_id,
		tgt.latitude = src.latitude,
		tgt.longitude = src.longitude,
		tgt.time_zone_id  = src.time_zone_id ,
		tgt.status = src.status,
		tgt.suburb = src.suburb,
		tgt.type = src.type,
		tgt.construction_year = src.construction_year,
		tgt.site_code = src.site_code,
		tgt.source_created_date = src.source_created_date,
		tgt.server = src.server,
		tgt.raw_json_value = src.raw_json_value,
		tgt._is_active = true,
		tgt._last_updated_at = SYSDATE(),
		tgt._stage_record_id = src._stage_record_id,
		tgt._loader_run_id = src._loader_run_id,
		tgt._ingested_at = src._ingested_at,
		tgt._staged_at = src._staged_at
  WHEN NOT MATCHED THEN
    INSERT (
		id,
		customer_id,
		portfolio_id,
		name,
		code,
		address,
		state,
		postal_code,
		country,
		number_of_floors,
		area,
		logo_id,
		latitude,
		longitude,
		time_zone_id ,
		status,
		suburb,
		type,
		construction_year,
		site_code,
		source_created_date,
		server,
		raw_json_value,
		_is_active,
		_created_at,
		_last_updated_at,
		_stage_record_id,
		_loader_run_id,
		_ingested_at,
		_staged_at
	  ) 
    VALUES (
		src.id,
		src.customer_id,
		src.portfolio_id,
		src.name,
		src.code,
		src.address,
		src.state,
		src.postal_code,
		src.country,
		src.number_of_floors,
		src.area,
		src.logo_id,
		src.latitude,
		src.longitude,
		src.time_zone_id ,
		src.status,
		src.suburb,
		src.type,
		src.construction_year,
		src.site_code,
		src.source_created_date,
		src.server,
		src.raw_json_value,
		true,
		SYSDATE(),
		SYSDATE(),
		src._stage_record_id,
		src._loader_run_id,
		src._ingested_at,
		src._staged_at
    );
    $$
;
