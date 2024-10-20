-- ------------------------------------------------------------------------------------------------------------------------------
-- Create Views
-- pivot each column from columns to rows; then join
-- ------------------------------------------------------------------------------------------------------------------------------
USE wil_automation_db;
USE SCHEMA data_compliance;
-- pivot the validation table
CREATE OR REPLACE VIEW data_compliance.pivot_validation AS
WITH obj AS (
   SELECT OBJECT_CONSTRUCT(*) o FROM data_compliance.raw_validation
)
SELECT CONCAT(o:FILENAME::varchar,'_',o:IFCGUID::varchar) AS unique_key, 
       o:FILENAME::varchar AS file_name,
       o:IFCGUID::varchar AS ifc_guid,
       f.key::varchar(2000) AS attribute, 
       f.value::varchar(1000) AS valid
FROM obj,
     lateral flatten (input => obj.o, mode => 'OBJECT') f
WHERE f.key NOT IN ('FILENAME','IFCGUID','_FILE_ROW_NUMBER','_INGESTED_AT','_STAGE_FILE_NAME')
;
-- pivot the data_files table
CREATE OR REPLACE VIEW data_compliance.pivot_data_files AS
WITH obj AS (
   SELECT OBJECT_CONSTRUCT(*) o FROM data_compliance.raw_data_files
)
SELECT CONCAT(o:FILENAME::varchar,'_',o:IFCGUID::varchar) AS unique_key, 
       o:FILENAME::varchar AS file_name,
       o:IFCGUID::varchar AS ifc_guid,
       f.key::varchar(2000) AS attribute, 
       f.value::varchar(1000) AS value
FROM obj,
     LATERAL FLATTEN (input => obj.o, mode => 'OBJECT') f
WHERE f.key NOT IN ('FILENAME','IFCGUID','_FILE_ROW_NUMBER','_INGESTED_AT','_STAGE_FILE_NAME')
;
-- join the two pivots
CREATE OR REPLACE VIEW data_compliance.compliance_all AS
SELECT 
 v.attribute,
 v.unique_key,
 v.file_name,
 v.valid,
 d.value,
 v.ifc_guid
FROM data_compliance.pivot_validation v
LEFT JOIN data_compliance.pivot_data_files d ON d.unique_key = v.unique_key AND d.attribute = v.attribute;
