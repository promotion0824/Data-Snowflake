-- ******************************************************************************************************************************
-- Stored procedure to persist the tenant engagement data
-- ******************************************************************************************************************************
USE DATABASE segment_events_db
;
CREATE OR REPLACE PROCEDURE transformed.insert_event_list_viewed_sp()
    RETURNS STRING
    LANGUAGE SQL
  AS
    $$
      BEGIN
			TRUNCATE TABLE transformed.event_list_viewed
			;
			INSERT INTO transformed.event_list_viewed ( 
				anonymous_id,
				building_id,
				building_name,
				client_name,
				context_locale,
				event,
				tenant_name,
				event_text,
				id,
				original_timestamp,
				uuid_ts,
				user_id,
				received_at
			)  
			SELECT 
				anonymous_id,
				building_id,
				building_name,
				client_name,
				context_locale,
				event,
				tenant_name,
				event_text,
				id,
				original_timestamp,
				uuid_ts,
				user_id,
				received_at
			FROM willow_experience_android.event_list_viewed
			;
			INSERT INTO transformed.event_list_viewed ( 
				anonymous_id,
				building_id,
				building_name,
				client_name,
				context_locale,
				event,
				tenant_name,
				event_text,
				id,
				original_timestamp,
				uuid_ts,
				user_id,
				received_at
			)  
			SELECT 
				anonymous_id,
				building_id,
				building_name,
				client_name,
				context_locale,
				event,
				tenant_name,
				event_text,
				id,
				original_timestamp,
				uuid_ts,
				user_id,
				received_at
			FROM willow_experience_ios.event_list_viewed
			;
			INSERT INTO transformed.event_list_viewed ( 
				anonymous_id,
				building_id,
				building_name,
				client_name,
				context_locale,
				event,
				tenant_name,
				event_text,
				id,
				original_timestamp,
				uuid_ts,
				user_id,
				received_at
			)  
			SELECT 
				anonymous_id,
				building_id,
				building_name,
				client_name,
				context_locale,
				event,
				tenant_name,
				event_text,
				id,
				original_timestamp,
				uuid_ts,
				user_id,
				received_at
			FROM willow_experience_website.event_list_viewed
			;
      END;
    $$
;
