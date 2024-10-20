-- ******************************************************************************************************************************
-- Create Account Details view
-- ******************************************************************************************************************************
CREATE OR REPLACE VIEW central_monitoring_db.published.account_details AS 
  SELECT 
    account_name,
    region,
    customer_identifier,
    snowflake_version,
    deployment_details,
    CONCAT(
      'https://', 
      LOWER(account_name), '.', 
      CASE REGION
        WHEN 'AZURE_AUSTRALIAEAST'
          THEN 'australia-east.azure'
        WHEN 'AZURE_EASTUS2'
          THEN 'east-us-2.azure'
        WHEN 'AZURE_WESTEUROPE'
          THEN 'west-europe.azure'
        ELSE 'UNKNOWN'
      END, 
      '.snowflakecomputing.com'
    ) AS account_url,
    _captured_at AS last_updated_at
  FROM raw.account_details
  QUALIFY ROW_NUMBER() OVER (PARTITION BY account_name ORDER BY _captured_at DESC) = 1 
;