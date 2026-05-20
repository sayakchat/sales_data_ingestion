# Data Model

## Core fact: sales line item

Grain: one transaction line per business date, transaction and line id.

Business key:

```text
business_date + transaction_id + transaction_line_id (  optional + unit_cost_centre_code )
```

Important measures:

| Column | Meaning |
|---|---|
| gross_sales | Gross line sales before discount and tax |
| discount_amount | Discount applied to the line |
| tax_amount | Tax component |
| net_sales | `gross_sales - discount_amount - tax_amount`, adjusted for cancelled/returned lines |
| quantity | Raw POS quantity |
| enhanced_quantity | Quantity excluding kitchen instructions and unpriced lines |

## Reposted transaction logic

Source systems can repost corrected versions of historical business-date transactions. The project chooses the latest record by:

1. `source_sequence`
2. `source_update_ts`
3. `extraction_ts`

This avoids double counting and is one of the highest-value rules in the platform.

## Time-grain model

For opening/closing analysis, transactions are bucketed into 15-minute intervals. This allows commercial teams to compare early and late trading windows against estimated energy and labour running cost.
