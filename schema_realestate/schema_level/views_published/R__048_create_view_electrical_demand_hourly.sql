-- ******************************************************************************************************************************
-- Deploy final view based on customer's configuration
-- ******************************************************************************************************************************
EXECUTE IMMEDIATE
$$
DECLARE 
	scenario1 STRING;
	scenario2 STRING;
	scenario3 STRING;
BEGIN
			SELECT TOP 1 telemetry_value INTO :scenario1
			--Scenario 1: published.utilivisor_tenant_power; aggregate to tenant/asset/15min;
			FROM transformed.telemetry
			WHERE date_local >= DATEADD('month',-1,date_local)
			AND dt_id IN (SELECT DISTINCT capability_id FROM transformed.capabilities_assets 
							WHERE model_id_asset = 'dtmi:com:willowinc:UtilityAccount;1'
								AND LOWER(asset_name) like '%utilivisor%')
            ;
	IF (scenario1 IS NOT NULL) THEN
		CREATE OR REPLACE VIEW published.electrical_demand_hourly AS
		SELECT 
		    date_local,
			date_time_local_15min,
			date_time_local_hour,
			power_consumption,
			power_consumption AS HOURLY_POWER_CONSUMPTION,
			asset_id,
			tenant_id,
			tenant_name,
			site_name,
			site_id,
			building_id,
			building_name
		FROM transformed.tenant_energy_utility_account;
	END IF;
	IF (scenario1 IS NULL) THEN
			SELECT TOP 1 telemetry_value INTO :scenario2
			--Scenario 2: use the transformed.agg_electrical_demand_hourly table;
			FROM transformed.telemetry
			WHERE date_local >= DATEADD('month',-1,date_local)
			AND dt_id IN (SELECT DISTINCT capability_id FROM transformed.electrical_metering_hierarchy 
							WHERE sensor_type = 'Power')
            ;
	END IF;
	IF (scenario1 IS NULL AND scenario2 IS NOT NULL) THEN
		CREATE OR REPLACE VIEW published.electrical_demand_hourly AS
		SELECT
		    date_local,
			date_time_local_hour AS date_time_local_15min,
			date_time_local_hour,
			power_consumption,
			power_consumption AS HOURLY_POWER_CONSUMPTION,
			asset_id,
			tenant_id,
			tenant_name,
			site_name,
			site_id,
			building_id,
			building_name 
		FROM published.tenant_electrical_demand_hourly;
	END IF;
	IF (scenario1 IS NULL AND scenario2 IS NULL) THEN
			SELECT TOP 1 telemetry_value INTO :scenario3
			--Scenario 3: published.tenant_electrical_metering_detail;
			FROM transformed.telemetry
			WHERE date_local >= DATEADD('month',-1,date_local)
			AND dt_id IN (SELECT DISTINCT capability_id FROM transformed.electrical_metering_hierarchy 
						WHERE sensor_type = 'Energy')
			;
			CREATE OR REPLACE VIEW published.electrical_demand_hourly AS
			SELECT
				date_local,
				date_time_local_hour AS date_time_local_15min,
				date_time_local_hour,
				power_consumption,
				power_consumption AS HOURLY_POWER_CONSUMPTION,
				asset_id,
				tenant_id,
				tenant_name,
				site_name,
				site_id,
				building_id,
				building_name 
			FROM published.tenant_electrical_demand_based_on_hourly_energy;
	END IF;
	SELECT :scenario1, :scenario2, :scenario3;
END;
$$
