-- ******************************************************************************************************************************
-- Create view and persist as a table in transformed
-- ******************************************************************************************************************************
CREATE OR REPLACE VIEW transformed.sustainability_resources_models AS
SELECT *
    FROM VALUES 
    ('Electricity', 'Actual', 'dtmi:com:willowinc:ActiveElectricalEnergySensor;1','eGrid 2021, US EPA'),
    ('Electricity', 'Metered', 'dtmi:com:willowinc:BilledActiveElectricalEnergy;1','eGrid 2021, US EPA'),
    ('Electricity', 'Metered', 'dtmi:com:willowinc:BilledElectricalCost;1',NULL),
    --Natural gas is reported in Energy
    ('Natural Gas', 'Actual', 'dtmi:com:willowinc:NaturalGasEnergySensor;1','US EIA'),
    ('Natural Gas', 'Metered','dtmi:com:willowinc:BilledNaturalGasEnergy;1','US EIA'),
    ('Natural Gas', 'Metered', 'dtmi:com:willowinc:BilledNaturalGasCost;1',NULL),
    --OR, if Natural gas is reported in Mass 
    ('Natural Gas', 'Actual', 'dtmi:com:willowinc:NaturalGasMassSensor;1','US EIA'),
    ('Natural Gas', 'Metered', 'dtmi:com:willowinc:BilledNaturalGasMass;1','US EIA'),
    --OR, if natural gas is reported in Volume
    ('Natural Gas', 'Actual', 'dtmi:com:willowinc:NaturalGasVolumeSensor;1','US EIA'),
    ('Natural Gas', 'Metered', 'dtmi:com:willowinc:BilledNaturalGasVolume;1','US EIA'),
    --Steam
    ('Steam','Actual', 'dtmi:com:willowinc:SteamMassSensor;1',NULL),
    ('Steam','Actual', 'dtmi:com:willowinc:SteamThermalEnergySensor;1',NULL),
    ('Steam','Metered', 'dtmi:com:willowinc:BilledSteamMass;1',NULL),
    ('Steam','Metered', 'dtmi:com:willowinc:BilledSteamThermalEnergy;1',NULL),
    ('Steam','Metered', 'dtmi:com:willowinc:BilledSteamCost;1',NULL),
    --Domestic Water
    ('Domestic Water','Actual', 'dtmi:com:willowinc:WaterVolumeSensor;1',NULL),
    ('Domestic Water','Metered', 'dtmi:com:willowinc:BilledWaterVolume;1',NULL),
    ('Domestic Water','Metered', 'dtmi:com:willowinc:BilledWaterCost;1',NULL),
    --Reclaim Water (Sometimes shown as Irrigation)
    ('Reclaim Water', 'Actual', 'dtmi:com:willowinc:ReclaimedWaterVolumeSensor;1',NULL),
    ('Reclaim Water', 'Metered', 'dtmi:com:willowinc:BilledReclaimedWaterVolume;1',NULL),
    ('Reclaim Water', 'Metered', 'dtmi:com:willowinc:BilledReclaimedWaterCost;1',NULL),
    --Sewage
    ('Sewage', 'Actual', 'dtmi:com:willowinc:SewageVolumeSensor;1',NULL),
    ('Sewage', 'Metered','dtmi:com:willowinc:BilledSewageVolume;1',NULL),
    ('Sewage', 'Metered','dtmi:com:willowinc:BilledSewerCost;1',NULL)
    AS v (resource_type, data_type, capablity_model_id, emissions_factor_source)
;
