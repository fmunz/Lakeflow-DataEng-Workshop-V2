# Lakeflow Data Engineering Workshop (V2)

Databricks's new Data Engineering course, rebuilt around **Lakeflow Spark Declarative Pipelines (SDP)**, the new **Lakeflow Pipelines Editor**, and **Genie Code** Agent mode.

## What you'll do

Three labs, ~110 minutes total:

- **Lab 1 — Bakehouse (hand-coded).** Build a streaming table in Python (modern `from pyspark import pipelines as dp` API) and a materialized view in SQL over `samples.bakehouse.sales_transactions`. Reference files in [`lab1-bakehouse/`](./lab1-bakehouse/).
- **Lab 2 — Wanderbricks (SQL, Genie-Code-generated).** Build a four-table fraud-detection pipeline from a single Genie Code prompt — AutoCDC over `samples.wanderbricks.booking_updates`, Auto Loader over JSON fraud flags in a shared volume, a join of payments, and a gold materialized view. Verify the generated SQL against the reference files in [`lab2-wanderbricks/`](./lab2-wanderbricks/) before letting it run.
- **Lab 3 — Gourmet Pipeline (Databricks Asset Bundle deploy).** Clone [`databricks/tmm/Lakeflow-Gourmet-Pipeline`](https://github.com/databricks/tmm/tree/main/Lakeflow-Gourmet-Pipeline) into your workspace, adjust two bundle variables (`catalog_name`, `prod_warehouse_id`) so it targets your own `<catalog>.<user>`, deploy + run the bundle from the Workspace UI.

See [Labguide.md](./Labguide.md) for the step-by-step exercises.

## Prerequisites

- A Databricks workspace with Unity Catalog and Serverless enabled.
- Partner-powered AI features enabled (Genie Code dependency).
- A pre-assigned schema `<catalog>.<user>` per attendee.
- The setup notebook has been run once to create the shared landing volume with seeded JSON fraud markers.

## Tech covered

- `CREATE OR REFRESH STREAMING TABLE` and `MATERIALIZED VIEW`
- `AUTO CDC INTO ... STORED AS SCD TYPE 1`
- `FROM STREAM read_files(...)` (Auto Loader)
- `from pyspark import pipelines as dp`, `@dp.table(...)`
- The new **Lakeflow Pipelines Editor** (asset browser, Run file / Run pipeline)
- **Genie Code** Agent mode for AI-assisted pipeline authoring
- **Databricks Asset Bundles** (DAB) — `databricks.yml` variables/targets, Workspace UI deploy (**Deployments** 🚀 pane)
- Git folder + sparse checkout for cloning a subfolder of a public repo

## Why V2

V1 of the course used Delta Live Tables (DLT) syntax (`import dlt`, `CREATE LIVE TABLE`) and a single-notebook pipeline editor. V2 rebuilds around:

- The **modern SDP API** (`pyspark.pipelines`) — `import dlt` is legacy.
- The **new multi-file Lakeflow Pipelines Editor** with explicit transformation / exploration asset types.
- **Genie Code** as a first-class authoring tool, with mandatory human-in-the-loop verification.
