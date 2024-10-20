-- ******************************************************************************************************************************
-- Stored procedure that persists aggregate data
-- ******************************************************************************************************************************

CREATE OR REPLACE PROCEDURE transformed.merge_customers_sp()
    RETURNS STRING
    LANGUAGE SQL
  AS
  $$
  BEGIN

    CREATE OR REPLACE TEMPORARY TABLE temp_customers AS 
    SELECT DISTINCT * FROM raw.json_customers_str;
    
    DELETE FROM transformed.customers c
    USING temp_customers 
    WHERE c.portfolio_id = temp_customers.json_value:portfolio_id::STRING;

    INSERT INTO transformed.customers ( 
        portfolio_id	          ,
        portfolio_name	        ,
        portfolio_features_json	,
        customer_id	            ,
        customer_name	          ,
        address1	              ,
        address2	              ,
        suburb	                ,
        post_code	              ,
        country	                ,
        status	                ,
        state	                  ,
        logo_id	                ,
        customer_features_json	,
        account_external_id	    ,
        models_of_interest_json	,
        model_of_interest_etag	,
        sigma_connection_id	    ,
        _last_updated_at
  ) 
      SELECT DISTINCT
        json_value:portfolio_id::STRING AS portfolio_id,
        json_value:portfolio_name::STRING AS portfolio_name,
        json_value:PortfolioFeaturesJson::VARIANT AS portfolio_features_json,
        json_value:Id::STRING AS customer_id,
        json_value:Name::STRING AS customer_name,
        json_value:Address1::STRING AS address1,
        json_value:Address2::STRING AS address2,
        json_value:Suburb::STRING AS suburb,
        json_value:Postcode::STRING AS post_code,
        json_value:Country::STRING AS country,
        json_value:Status::STRING AS status,
        json_value:State::STRING AS state,
        json_value:LogoId::STRING AS logo_id,
        json_value:FeaturesJson::VARIANT AS customer_features_json,
        json_value:AccountExternalId::STRING AS account_external_id,
        json_value:ModelsOfInterestJson::VARIANT AS models_of_interest_json,
        json_value:ModelsOfInterestETag::VARIANT AS model_of_interest_etag,
        json_value:SigmaConnectionId::STRING AS sigma_connection_id,
        SYSDATE()
      FROM temp_customers;
  END;
$$
;