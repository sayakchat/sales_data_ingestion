-- Analytics view: combo pricing analysis.
-- The aim is to compare the realised unit price of products sold inside a combo versus
-- the realised unit price when sold separately.

CREATE OR REPLACE VIEW analytics.vw_combo_analytics AS
WITH priced_lines AS (
    SELECT
        product_id,
        product_name,
        is_combo,
        net_sales / NULLIF(enhanced_quantity, 0) AS unit_net_price,
        enhanced_quantity
    FROM silver_sales_line_items_business
    WHERE enhanced_quantity > 0
), standalone AS (
    SELECT
        product_id,
        product_name,
        AVG(unit_net_price) AS standalone_avg_unit_net_price
    FROM priced_lines
    WHERE is_combo = false
    GROUP BY product_id, product_name
), combo AS (
    SELECT
        product_id,
        product_name,
        AVG(unit_net_price) AS combo_avg_unit_net_price,
        SUM(enhanced_quantity) AS combo_units
    FROM priced_lines
    WHERE is_combo = true
    GROUP BY product_id, product_name
)
SELECT
    c.product_id,
    c.product_name,
    c.combo_avg_unit_net_price,
    s.standalone_avg_unit_net_price,
    c.combo_avg_unit_net_price - s.standalone_avg_unit_net_price AS combo_vs_standalone_delta,
    ((c.combo_avg_unit_net_price - s.standalone_avg_unit_net_price) / s.standalone_avg_unit_net_price) * 100 AS combo_discount_percent,
    c.combo_units
FROM combo c
LEFT JOIN standalone s
    ON c.product_id = s.product_id;
