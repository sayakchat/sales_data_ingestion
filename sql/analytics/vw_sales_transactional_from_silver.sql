-- Analytics layer built directly from Silver, not Gold.
-- This gives BI users denormalised fields while retaining transaction-line drill-through.

CREATE OR REPLACE VIEW analytics.vw_sales_transactional_from_silver AS
SELECT
    s.business_date,
    s.country_code,
    s.location_id,
    s.unit_id,
    u.unit_name,
    s.transaction_id,
    s.transaction_line_id,
    s.transaction_completed_ts,
    s.quarter_hour_start_ts,
    s.product_id,
    p.product_name,
    p.product_category,
    s.gross_sales,
    s.discount_amount,
    s.tax_amount,
    s.net_sales,
    s.enhanced_quantity,
    s.discount_type,
    s.discount_name,
    b.daily_budget_net_sales,
    b.forecast_net_sales
FROM silver.sales_line_items_business s
LEFT JOIN silver.dim_unit u
    ON s.country_code = u.country_code
   AND s.location_id = u.location_id
   AND s.unit_id = u.unit_id
LEFT JOIN silver.dim_product p
    ON s.product_id = p.product_id
LEFT JOIN silver.daily_budget_forecast b
    ON s.business_date = b.business_date
   AND s.unit_id = b.unit_id;
