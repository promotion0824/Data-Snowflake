-- ******************************************************************************************************************************
-- Stored procedure to persist the tenant engagement data
-- ******************************************************************************************************************************
USE DATABASE segment_events_db
;
CREATE OR REPLACE PROCEDURE transformed.insert_event_details_viewed_sp()
    RETURNS STRING
    LANGUAGE SQL
  AS
    $$
      BEGIN
			TRUNCATE TABLE transformed.event_details_viewed
			;
			INSERT INTO transformed.event_details_viewed ( 
				anonymous_id,
				building_id,
				building_name,
				client_name,
				context_locale,
				event,
				event_category,
				event_text,
				id,
				original_timestamp,
				received_at,
				sent_at,
				tenant_name,
				user_id,
				uuid_ts
			)  
			SELECT 
				anonymous_id,
				building_id,
				building_name,
				client_name,
				context_locale,
				event,
				event_category,
				event_text,
				id,
				original_timestamp,
				received_at,
				sent_at,
				tenant_name,
				user_id,
				uuid_ts
			FROM willow_experience_android.event_details_viewed
			;
			INSERT INTO transformed.event_details_viewed ( 
				anonymous_id,
				building_id,
				building_name,
				client_name,
				context_locale,
				event,
				event_category,
				event_text,
				id,
				original_timestamp,
				received_at,
				sent_at,
				tenant_name,
				user_id,
				uuid_ts
			)  
			SELECT 
				anonymous_id,
				building_id,
				building_name,
				client_name,
				context_locale,
				event,
				event_category,
				event_text,
				id,
				original_timestamp,
				received_at,
				sent_at,
				tenant_name,
				user_id,
				uuid_ts
			FROM willow_experience_ios.event_details_viewed
			;
			INSERT INTO transformed.event_details_viewed ( 
				anonymous_id,
				building_id,
				building_name,
				client_name,
				context_locale,
				event,
				event_category,
				event_text,
				id,
				original_timestamp,
				received_at,
				sent_at,
				tenant_name,
				user_id,
				uuid_ts
			)  
			SELECT 
				anonymous_id,
				building_id,
				building_name,
				client_name,
				context_locale,
				event,
				event_category,
				event_text,
				id,
				original_timestamp,
				received_at,
				sent_at,
				tenant_name,
				user_id,
				uuid_ts
			FROM willow_experience_website.event_details_viewed
			;
      END;
    $$
;