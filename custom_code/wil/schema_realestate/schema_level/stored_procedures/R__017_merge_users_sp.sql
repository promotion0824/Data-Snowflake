-- ******************************************************************************************************************************
-- Stored procedure that merges into insights
-- This is called via transformed.merge_insights_tk which is scheduled by CRON
-- USAGE:  CALL transformed.merge_insights_sp(SYSTEM$CURRENT_USER_TASK_NAME());
-- ******************************************************************************************************************************

CREATE OR REPLACE PROCEDURE transformed.merge_users_sp(task_name VARCHAR)
    RETURNS STRING
    LANGUAGE SQL
    EXECUTE AS CALLER
  AS
    $$
    MERGE INTO transformed.users AS tgt 
    USING (
      SELECT
      json_value:customer::STRING AS customer,
      json_value:Email::STRING AS email,
      json_value:FirstName::STRING AS first_name,
      json_value:GroupTypeId::STRING AS group_type_id,
      json_value:Id::STRING AS id,
      json_value:LastName::STRING AS last_name,
      json_value:Status::STRING AS status,
      json_value:groupId::STRING AS group_id,
      json_value:group_name::STRING AS group_name,
      json_value:group_type::STRING AS group_type, 
      json_value:role_description::STRING AS role_description,
      json_value:role_name::STRING AS role_name,
      json_value:CreatedDate::STRING AS created_date,
      _loader_run_id,
      _ingested_at,
      _staged_at
      FROM raw.json_users_str
      QUALIFY ROW_NUMBER() OVER (PARTITION BY email,customer ORDER BY _ingested_at DESC) = 1
    ) AS src
      ON (tgt.email = src.email)
      AND (tgt.customer = src.customer)
    WHEN MATCHED THEN
      UPDATE 
      SET 
      tgt.customer = src.customer,
      tgt.email = src.email,
      tgt.first_name = src.first_name,
      tgt.last_name = src.last_name,
      tgt.status = src.status,
      tgt.id = src.id,
      tgt.group_id = src.group_id,
      tgt.group_name = src.group_name,
      tgt.group_type_id = src.group_type_id,
      tgt.group_type = src.group_type,
      tgt.role_name = src.role_name,
      tgt.role_description = src.role_description,
      tgt.created_date = src.created_date,
      tgt._last_updated_at = SYSDATE(),
      tgt._loader_run_id = src._loader_run_id,
      tgt._ingested_at = src._ingested_at
    WHEN NOT MATCHED THEN
      INSERT (
      customer,
      email,
      first_name,
      last_name,
      status,
      id,
      group_id,
      group_name,
      group_type_id,
      group_type,
      role_name,
      role_description,
      created_date,
      _created_at,
      _last_updated_at,
      _loader_run_id,
      _ingested_at
        ) 
      VALUES (
      src.customer,
      src.email,
      src.first_name,
      src.last_name,
      src.status,
      src.id,
      src.group_id,
      src.group_name,
      src.group_type_id,
      src.group_type,
      src.role_name,
      src.role_description,
      src.created_date,
      SYSDATE(),
      SYSDATE(),
      src._loader_run_id,
      src._ingested_at
      );
    $$
;