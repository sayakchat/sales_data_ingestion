-- Analytics view: unit-level sales versus running cost in 15-minute buckets.
-- Business use case: decide whether specific units should open later or close earlier.

CREATE OR REPLACE VIEW analytics.vw_unit_sales_15_min_break_even AS
SELECT
    business_date,
    country_code,
    location_id,
    unit_id,
    quarter_hour_start_ts,
    date_format(quarter_hour_start_ts, 'HH:mm') AS quarter_hour_key,
    net_sales,
    enhanced_quantity,
    transaction_count,
    estimated_running_cost,
    profitability_gap,
    trading_recommendation
FROM gold_unit_quarter_hour_sales;
