-- Analytics view: ATV and enhanced quantity.
-- Enhanced quantity excludes non-priced lines such as kitchen instructions to avoid inflating
-- item counts and distorting Average Transaction Value.

CREATE OR REPLACE VIEW analytics.vw_enhanced_quantity_atv AS
SELECT
    business_date,
    country_code,
    location_id,
    unit_id,
    COUNT(DISTINCT transaction_id) AS transaction_count,
    SUM(net_sales) AS net_sales,
    SUM(enhanced_quantity) AS enhanced_quantity,
    SUM(net_sales) / NULLIF(COUNT(DISTINCT transaction_id), 0) AS average_transaction_value,
    SUM(net_sales) / NULLIF(SUM(enhanced_quantity), 0) AS net_sales_per_enhanced_item
FROM silver_sales_line_items_business
GROUP BY business_date, country_code, location_id, unit_id;
