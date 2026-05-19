-- Analytics view: discount strategy by product category, geography and unit.
-- Business use case: identify discount names and locations that generate strong sales uplift
-- versus those that erode margin without enough transaction volume.

CREATE OR REPLACE VIEW analytics.vw_discount_strategy AS
SELECT
    country_code,
    location_id,
    unit_id,
    discount_type,
    discount_name,
    product_category,
    gross_sales,
    discount_amount,
    net_sales,
    transaction_count,
    discount_rate_percent,
    net_sales_per_transaction,
    CASE
        WHEN discount_rate_percent > 20 AND net_sales_per_transaction < 5 THEN 'review_discount_depth'
        WHEN transaction_count > 100 AND net_sales_per_transaction >= 8 THEN 'candidate_for_promotion'
        ELSE 'monitor'
    END AS commercial_action
FROM gold_discount_strategy;
