
--Objects we didn't end up needing (SB: Ended up running this manually to remove this object)
--DROP NOTIFICATION INTEGRATION IF EXISTS EXT_STAGE_MAPPED_RAW_DATA_{{ environment }}_NIN;

--Old name of stage
DROP STAGE IF EXISTS raw.data_loader_mapped_raw_data;

