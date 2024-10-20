-- ------------------------------------------------------------------------------------------------------------------------------
-- Set Secure on views to be shared
-- ------------------------------------------------------------------------------------------------------------------------------


ALTER VIEW published.DATES SET SECURE;
ALTER VIEW published.DATE_HOUR SET SECURE;
ALTER VIEW published.DATE_HOUR_15MIN SET SECURE;
ALTER VIEW published.TWINS_STATUS SET SECURE;
ALTER VIEW published.TWINS_RELATIONSHIPS SET SECURE;
ALTER VIEW published.ONTOLOGY_BUILDINGS SET SECURE;
ALTER VIEW published.SITES SET SECURE;

EXECUTE IMMEDIATE
$$
DECLARE 
	currentdb STRING;
BEGIN
	SELECT CURRENT_DATABASE() INTO :currentdb From dual;
	IF (currentdb='PRD_DB') THEN
        GRANT SELECT ON VIEW published.DATES TO SHARE external_share;
        GRANT SELECT ON VIEW published.DATE_HOUR TO SHARE external_share;
        GRANT SELECT ON VIEW published.DATE_HOUR_15MIN TO SHARE external_share;
        GRANT SELECT ON VIEW published.TWINS_STATUS TO SHARE external_share;
        GRANT SELECT ON VIEW published.TWINS_RELATIONSHIPS TO SHARE external_share;
        GRANT SELECT ON VIEW published.ONTOLOGY_BUILDINGS TO SHARE external_share;
        GRANT SELECT ON VIEW published.SITES TO SHARE external_share;
	END IF;
END;
$$


