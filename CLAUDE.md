# Lakeflow-DataEng-Workshop-V2 — Project Instructions for Claude

## Project overview
Four-lab Databricks training on Lakeflow Spark Declarative Pipelines (SDP) and direct ingest,
delivered in the new Lakeflow Pipelines Editor. Attendees each have a pre-assigned schema
`workshop.<user>`. A shared landing volume (for Lab 2) and a shared Zerobus target table
(for Lab 4) are preseeded by a single setup notebook.

- **Lab 1 — Bakehouse, hand-coded.** Streaming table in **Python** (1a), materialized view in **SQL** (1b),
  both over `samples.bakehouse.sales_transactions`. Step 1c adds data-quality expectations to
  `sales_stats` — a single MV with three different `CONSTRAINT ... EXPECT` clauses, one per
  violation behavior (log / drop row / fail update). Plus an Iceberg side-quest (Steps 1d/1e):
  a managed-Iceberg CTAS (`global_sales_gold`, top-5 locations) run **outside** the pipeline,
  read back from a small PyIceberg Exploration notebook via the UC Iceberg REST Catalog.
  Source files in `lab1-bakehouse/`.
- **Lab 2 — Wanderbricks, SQL, Genie-Code-generated (verified).** AutoCDC on
  `samples.wanderbricks.booking_updates`, Auto Loader on JSON fraud markers,
  streaming table on `samples.wanderbricks.payments`, and a three-way-join gold MV.
  Reference files in `lab2-wanderbricks/`.
- **Lab 3 — Gourmet Pipeline (external DAB deploy).** Clones
  `databricks/tmm/Lakeflow-Gourmet-Pipeline` into the workspace via Git folder + sparse
  checkout, per-student overrides of `catalog_name` / `prod_warehouse_id` (and optionally
  `schema_name`) in `databricks.yml`, then deploys through the **Deployments** (🚀) pane.
  Source lives in the external repo, not this one — we reference it, never fork it.
- **Lab 4 — Zerobus direct ingest.** Attendee Exploration notebook pushes one
  `{id, city, temp}` record into the shared Delta table `workshop.zerobus.course_temp` via
  the **Zerobus REST API** (serverless-friendly; the gRPC SDK can't pip-install on
  serverless compute). Credentials come from the `workshop` Databricks secret scope
  (`zerobus_client_id`, `zerobus_client_secret`, `zerobus_endpoint`, `zerobus_workspace_id`,
  `zerobus_workspace_url`) — attendees never see raw SP secrets. Reference file in
  `lab4-zerobus/send_temperature.py`.

## Language split (MUST preserve)
- Lab 1a (streaming table) — **Python** (`@dp.table` + `spark.readStream.table(...)`).
- Lab 1b (materialized view) — **SQL** (`CREATE OR REFRESH MATERIALIZED VIEW`).
- Lab 1c (expectations on the MV) — **SQL** (three `CONSTRAINT ... EXPECT` clauses: one LOG/default, one `ON VIOLATION DROP ROW`, one `ON VIOLATION FAIL UPDATE` — all in a single MV definition, not three separate code options).
- Lab 1d (managed Iceberg CTAS, **outside** the pipeline) — **SQL** (`CREATE OR REPLACE TABLE ... USING ICEBERG AS SELECT ...`).
- Lab 1e (Iceberg reader, **outside** the pipeline) — small **Python** Exploration notebook using `pyiceberg`. This is a client-side reader, not pipeline code — the in-pipeline language split is untouched.
- Lab 2 — every file is **SQL** (streaming tables, Auto Loader, AutoCDC flow, gold MV).
- Net effect for **in-pipeline** code: Python appears exactly once in the whole course (Lab 1a); everything else in the pipeline is SQL. This is deliberate so Lab 2 aligns with what Genie Code generates. Do not "unify" Lab 1 to all-Python or all-SQL.

## SDP code conventions (MUST follow)
- Use **`CREATE OR REFRESH`** (never `CREATE OR REPLACE`) for streaming tables and
  materialized views. `CREATE OR REPLACE TABLE` **is** allowed for the Lab 1c managed
  Iceberg CTAS, because that runs outside SDP in the SQL editor — plain Spark SQL semantics apply.
- Python: `from pyspark import pipelines as dp`. Never use legacy `import dlt`,
  `dlt.read`, `dlt.read_stream`, or `dlt.apply_changes`.
- Inside `@dp.table`, use `spark.read.table(...)` for MV / batch reads and
  `spark.readStream.table(...)` for streaming reads.
- Auto Loader in SQL streaming tables: `FROM STREAM read_files('/Volumes/...', format => 'json')`.
  `STREAM` is required; plain `read_files(...)` is batch and will fail in a streaming table.
- AutoCDC: `CREATE FLOW ... AS AUTO CDC INTO ... KEYS (...) SEQUENCE BY ... STORED AS SCD TYPE 1|2`.
- Serverless pipelines only (no classic clusters).
- Unity Catalog only.

## Source data (read-only)
- `samples.bakehouse.sales_transactions` — 3,333 rows, 6 products, 3 payment methods.
- `samples.wanderbricks.booking_updates` — the only native CDC feed in the dataset
  (keys `booking_id`, sequence `updated_at`, per-event `booking_update_id`).
- `samples.wanderbricks.payments` — joined with bookings via `booking_id`.

## Destination conventions
- Catalog: `workshop` (fixed — do not replace with a placeholder).
- Per-attendee schema: `workshop.<user>` (exists; setup notebook never creates/mutates it).
- Shared landing volume: `/Volumes/workshop/shared/landing/booking_fraud_flags/`
  — JSON fraud markers keyed by `booking_id`.

## Setup-notebook responsibilities (MUST include)
The setup notebook (`setup_workshop.py`, run once per workshop) is split into two parts.

**Part A — Lab 2 shared assets:**
1. Create schema `workshop.shared` if it does not exist.
2. Create volume `workshop.shared.landing` if it does not exist.
3. Create folder `booking_fraud_flags/` in the volume and seed JSON fraud markers
   for **3%** of distinct `booking_id`s from `samples.wanderbricks.booking_updates`.
4. **Grant `USE_SCHEMA` on `workshop.shared` to group `account users`**
   (so attendees can see the volume).
5. **Grant `READ_VOLUME` on `workshop.shared.landing` to group `account users`**
   (read-only — no `WRITE_VOLUME`).

**Part B — Lab 4 Zerobus provisioning** (skipped if `zerobus_region` widget is blank):
1. Create schema `workshop.zerobus` and managed Delta table `workshop.zerobus.course_temp`
   with exactly `id STRING, city STRING, temp FLOAT`. Zerobus does not create tables.
2. Create (or reuse) a workspace service principal `workshop-zerobus-sp`. Always generate
   a fresh OAuth client secret on each run — secret_scope writes overwrite, so attendees
   always read a current value.
3. Grant the SP: `USE CATALOG` on `workshop`, `USE SCHEMA` on `workshop.zerobus`,
   `MODIFY + SELECT` on `workshop.zerobus.course_temp`. Nothing broader — this bounds
   the blast radius if the client_secret ever leaks.
4. Create secret scope `workshop` and store five keys: `zerobus_client_id`,
   `zerobus_client_secret`, `zerobus_endpoint`, `zerobus_workspace_id`, `zerobus_workspace_url`.
5. Grant READ on the scope to `account users`. Attendees then read via
   `dbutils.secrets.get("workshop", ...)` — no credentials in notebook source.

**Global rules:**
6. Never grant anything on the individual per-attendee schemas.
7. Be idempotent — safe to re-run.

## UI conventions (new Lakeflow Pipelines Editor)
- Create: sidebar **New → ETL Pipeline**.
- Default catalog + schema: selector right of the pipeline name.
- Add source files: asset browser **Add → Transformation** (or **Add → Exploration** for
  non-pipeline notebooks).
- Run: **Run file** (single file) or **Run pipeline** (whole DAG).
- Genie Code: **Genie Code** button upper-right of workspace, **Agent** toggle lower-right
  of the pane. Never click *Always allow* in teaching material — verification each step is the pedagogy.

## When editing lab content
- Preserve the Lab 1 (Bakehouse, Python + SQL, hand-coded) vs Lab 2 (Wanderbricks, SQL, Genie-Code) split.
- If a new table is added, update both tables in the lab guide (wrap-up + course arc) and the
  verification plan in the instructor section.
- Keep Lab 2's single Genie Code prompt self-contained. If you change any of the four target
  files, update the prompt and all four reference code blocks in the same commit.
- Any doc references should link to `docs.databricks.com/aws/en/ldp/...` or
  `docs.databricks.com/aws/en/genie-code/...` and include the doc's "last updated" date.

## Lab 3 specifics
- Upstream source of truth: `https://github.com/databricks/tmm/tree/main/Lakeflow-Gourmet-Pipeline`.
- Per-student variables students MUST adjust in `databricks.yml`:
  - `catalog_name` — default is `daiwt_gourmet`; change to `workshop`.
  - `prod_warehouse_id` — default is an example ID that likely doesn't exist in the workshop workspace; replace with a real one from **SQL Warehouses**.
  - `schema_name` — default is `${workspace.current_user.short_name}`; leave it if that matches the attendee's `<user>` schema, otherwise override to the literal `<user>` value.
- Gotcha: dashboard SQL cannot be parameterized yet — if the attendee changes catalog/schema away from defaults, they must manually replace `daiwt_gourmet` in `resources/dashboard_gourmet_aibi.yml` and `src/aibi_dashboard.json`.
- Do NOT add a copy of the Gourmet Pipeline code to this repo. Lab 3 points to the upstream repo and students clone via Workspace **Create → Git folder** with sparse checkout path `Lakeflow-Gourmet-Pipeline`.

## Files in this repo
- `README.md` — course overview.
- `Labguide.md` — attendee-facing lab guide.
- `CLAUDE.md` — this file.
- `.gitignore` — standard Databricks/Python ignores.
- `setup_workshop.py` — instructor-run setup notebook (Part A: Lab 2 shared volume + seed; Part B: Lab 4 Zerobus target table, SP, grants, secret scope).
- `lab1-bakehouse/` — Lab 1 reference files: pipeline transformations (`sales_transactions.py`, `sales_stats.sql`) plus the Iceberg side-quest (`global_sales_gold.sql` CTAS and the `read_global_sales_gold.py` Databricks-notebook-source PyIceberg reader).
- `lab2-wanderbricks/` — Lab 2 reference SQL files (`bookings_current.sql`, `booking_fraud_flags.sql`, `payments.sql`, `booking_fraud_summary.sql`).
- `lab4-zerobus/` — Lab 4 reference file: `send_temperature.py` (Databricks notebook source). Payload-construction and POST logic are in a single "DO NOT MODIFY" cell; attendees only change the two widgets.
- `lab2-wanderbricks/` — Lab 2 reference transformation files (four `.sql` files).
