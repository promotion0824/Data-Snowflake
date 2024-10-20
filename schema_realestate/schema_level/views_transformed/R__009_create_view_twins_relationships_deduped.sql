--------------------------------------------------------------------------------------------------------------------------------
/*
  Create view that contains deduplicated twins relationships

  For some reason there are multiple non-deleted records with the same source_twin_id, target_twin_id, 
  and relationship_name (different relationship_id). These records exist in ADX as well but the deduplication 
  happens in DedupRelationshipsView but because we do incremental load and records are never marked as deleted,
  this is not picked up by the pipeline.

  TODO: Need to investigate why is this happening and whether is this necessary and whether deduplication can happen 
  as part of the ELT process. We should also consider materializing this view should it still be needed.
*/
-- -----------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW transformed.twins_relationships_deduped 
  AS
    SELECT *
    FROM transformed.twins_relationships
    QUALIFY ROW_NUMBER() OVER (PARTITION BY source_twin_id, target_twin_id, relationship_name ORDER BY export_time DESC) = 1;