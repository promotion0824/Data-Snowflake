CREATE OR REPLACE VIEW central_monitoring_db.published.embedded_widgets AS
WITH cte_widget AS (
SELECT 
    widget_id,
    site_id,
    portfolio_id,
    customer_id,
    position,
    type,
    TRY_PARSE_JSON(metadata) as meta_data,
    COALESCE(meta_data:name::STRING, meta_data:Name::STRING) AS report_name,
    meta_data:category::STRING AS category,
    meta_data:embedLocation AS embed_location,
    meta_data:embedGroup::VARIANT AS embed_group,
    COALESCE(meta_data:embedPath, meta_data:EmbedPath) AS embed_path,
    COALESCE(meta_data:groupId::STRING, meta_data:GroupId::STRING) AS group_id,
    COALESCE(meta_data:reportId::STRING, meta_data:ReportId::STRING) AS report_id,
    COALESCE(meta_data:name::STRING, meta_data:Name::STRING) AS pbi_name
FROM prd_db.transformed.widgets
)
SELECT 
    widget_id,
    COALESCE(c.customer_id,w.customer_id) AS customer_id,
    c.customer_name,
    w.site_id,
    s.name AS site_name,
    COALESCE(s.portfolio_id,w.portfolio_id) AS portfolio_id,
    c.portfolio_name,
    position,
    w.type,
    COALESCE(report_name,embed_group[0]:name::STRING) AS report_name,
    category,
    embed_location,
    COALESCE(embed_path,embed_group[0]:embedPath::STRING) AS embed_path,
    group_id,
    report_id,
    pbi_name,
    meta_data,
    embed_group[0]:name::STRING AS name1,
    embed_group[0]:view::STRING AS view1,
    embed_group[0]:order::STRING AS order1,
    embed_group[0]:embedPath::STRING AS embedPath1,
    embed_group[1]:name::STRING AS name2,
    embed_group[1]:view::STRING AS view2,
    embed_group[1]:order::STRING AS order2,
    embed_group[1]:embedPath::STRING AS embedPath2,
    embed_group
FROM cte_widget w
LEFT JOIN prd_db.transformed.site_core_sites s on w.site_id = s.id
LEFT JOIN prd_db.transformed.customers c on IFNULL(s.portfolio_id,w.portfolio_id) = c.portfolio_id;