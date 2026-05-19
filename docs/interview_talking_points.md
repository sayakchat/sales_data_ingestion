# Interview Talking Points

## 1. Metadata-driven ingestion

Instead of creating one ADF pipeline per Oracle table, I would use a control table to define the source schema, table, target entity, load pattern, primary keys and watermark. This improves maintainability and allows new source objects to be onboarded through metadata.

## 2. Reposted transaction dedupe

Retail sales systems often correct records after the original business date. If the lakehouse simply appends every extract, the same transaction line can be counted more than once. I solve this by defining a business key and selecting the latest source version using sequence and update timestamp.

## 3. Business semantics in Silver

Bronze should keep raw evidence. Silver is where I apply business definitions: net sales, returns, cancellations, enhanced quantity and quarter-hour buckets.

## 4. Gold models aligned to decisions

Gold is not just a technical aggregate. Each gold model maps to a decision:

- Unit 15-minute sales: opening and closing optimisation.
- Discount strategy: promote or remove offers.
- Combo analytics: evaluate product pricing inside combos.
- Enhanced quantity ATV: reliable transaction-value reporting.

## 5. Cost and performance optimisation

I would discuss partitioning by business date, clustering by country/unit, incremental processing, Auto Loader checkpoints, job scheduling, and table maintenance. This connects engineering design to FinOps and platform sustainability.

## 6. Lead-level behaviours

This project can also be used to discuss engineering standards, governance, code review, CI/CD, stakeholder workshops, roadmap ownership, and mentoring distributed data teams.
