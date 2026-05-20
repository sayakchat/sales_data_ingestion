# Data Quality Contract

## Bronze checks

| Rule | Action | Reason |
|---|---|---|
| transaction_id  (guestcheckid) is not null | flag / expectation | Required for dedupe and reconciliation |
| business_date is not null | flag / expectation | Required for partitioning and daily reporting |
| completed gross sales is non-negative | flag | Negative completed sales usually indicate a status issue |

## Silver checks

| Rule | Action | Reason |
|---|---|---|
| one latest record per sales line key | enforce by window dedupe | Prevents reposted records from double counting |
| net_sales is not null | expectation | Required for reporting |
| enhanced_quantity >= 0 | expectation | Required for ATV and quantity analytics |
| cancelled sales net to zero | business rule | Prevents cancelled transactions from inflating sales |
| returned sales are negative | business rule | Allows natural netting against original sales |

## Reconciliation ideas

- Compare Oracle source row counts to Bronze row counts by entity and extraction run.
- Compare Bronze-to-Silver dropped/overwritten rows by business date.
- Compare Gold total net sales to Silver total net sales by country and business date.
- Alert when a country is missing from a scheduled six-daily run.
- Alert when a DQ Check Fail using a ref_notification table with technical owner and functional owner
