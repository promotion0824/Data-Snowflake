-- ******************************************************************************************************************************
-- Create view
-- ******************************************************************************************************************************

CREATE OR REPLACE VIEW transformed.insight_types AS 
-- source: https://github.com/WillowInc/TwinPlatform/blob/main/extensions/real-estate/back-end/InsightCore/src/InsightCore/Models/InsightType.cs
SELECT *
FROM (VALUES 
(1,'Fault'),
(2,'Energy'),
(3,'Alert'),
(4,'Note'),
(5,'GoldenStandard'),
(6,'Infrastructure'),
(7,'IntegrityKpi'),
(8,'EnergyKpi'),
(9,'EdgeDevice'),
(10,'DataQuality'),
(11,'Commissioning'),
(12,'Comfort'),
(13,'Wellness'),
(14,'Calibration'),
(15,'Diagnostic'),
(16,'Predictive')
) AS v (insight_type_id, insight_type)
;
