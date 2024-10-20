-- ******************************************************************************************************************************
-- Task to unload sites to stage
-- ******************************************************************************************************************************

ALTER TASK IF EXISTS transformed.merge_site_core_sites_tk SUSPEND;

CREATE OR REPLACE TASK transformed.unload_sites_tk
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
  USER_TASK_TIMEOUT_MS = 1200000
  AFTER transformed.merge_site_core_sites_tk
AS
  COPY INTO @raw.ADHOC_ESG/site_core_sites/ FROM transformed.sites_long_lat file_format = (TYPE = 'JSON', COMPRESSION = GZIP) OVERWRITE = TRUE;

ALTER TASK IF EXISTS transformed.unload_sites_tk RESUME;
ALTER TASK IF EXISTS transformed.merge_site_core_sites_tk RESUME;