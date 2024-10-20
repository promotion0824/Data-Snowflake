-- ******************************************************************************************************************************
-- Stored procedure to persist the tenant engagement data
-- ******************************************************************************************************************************
USE DATABASE segment_events_db
;
CREATE OR REPLACE PROCEDURE transformed.insert_hid_access_granted_sp()
    RETURNS STRING
    LANGUAGE SQL
  AS
    $$
      BEGIN
			TRUNCATE TABLE transformed.hid_access_granted
			;
			INSERT INTO transformed.hid_access_granted ( 
				anonymous_id,
				building_id,
				building_name,
				client_name,
				event,
				event_text,
				id,
				original_timestamp,
				received_at,
				sent_at,
				tenant_name,
				user_id,
				uuid_ts
			)  SELECT 
				anonymous_id,
				building_id,
				building_name,
				client_name,
				event,
				event_text,
				id,
				original_timestamp,
				received_at,
				sent_at,
				tenant_name,
				user_id,
				uuid_ts
			FROM willow_experience_android.hid_access_granted
			;
			INSERT INTO transformed.hid_access_granted ( 
				anonymous_id,
				building_id,
				building_name,
				client_name,
				event,
				event_text,
				id,
				original_timestamp,
				received_at,
				sent_at,
				tenant_name,
				user_id,
				uuid_ts
			)  SELECT 
				anonymous_id,
				building_id,
				building_name,
				client_name,
				event,
				event_text,
				id,
				original_timestamp,
				received_at,
				sent_at,
				tenant_name,
				user_id,
				uuid_ts
			FROM willow_experience_ios.hid_access_granted
			;
      END;
    $$
;
