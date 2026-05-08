# Lakeflow-DataEng-Workshop-V2 — Project Instructions for Claude

## Project overview
Two-lab Databricks training on Lakeflow Spark Declarative Pipelines (SDP), delivered in the
new Lakeflow Pipelines Editor. Attendees each have a pre-assigned schema `<catalog>.<user>`
in an existing workshop catalog. A shared landing volume is preseeded by a setup notebook.

- **Lab 1 — Bakehouse, hand-coded.** Streaming table in **Python**, materialized view in **SQL**,
  both over `samples.bakehouse.sales_transactions`. Source files in `lab1-bakehouse/`.
- **Lab 2 — Wanderbricks, SQL, Genie-Code-generated (verified).** AutoCDC on
  `samples.wanderbricks.booking_updates`, Auto Loader on JSON fraud markers,
  streaming table on `samples.wanderbricks.payments`, and a three-way-join gold MV.
  Reference files in `lab2-wanderbricks/`.
- **Lab 3 — Gourmet Pipeline (external DAB deploy).** Clones
  `databricks/tmm/Lakeflow-Gourmet-Pipeline` into the workspace via Git folder + sparse
  checkout, per-student overrides of `catalog_name` / `prod_warehouse_id` (and optionally
  `schema_name`) in `databricks.yml`, then deploys through the **Deployments** (🚀) pane.
  Source lives in the external repo, not this one — we reference it, never fork it.

## Language split (MUST preserve)
- Lab 1a (streaming table) — **Python** (`@dp.table` + `spark.readStream.table(...)`).
- Lab 1b (materialized view) — **SQL** (`CREATE OR REFRESH MATERIALIZED VIEW`).
- Lab 2 — every file is **SQL** (streaming tables, Auto Loader, AutoCDC flow, gold MV).
- Net effect: Python appears exactly once in the whole course (Lab 1a); everything else is SQL. This is deliberate so Lab 2 aligns with what Genie Code generates. Do not "unify" Lab 1 to all-Python or all-SQL.

## SDP code conventions (MUST follow)
- Use **`CREATE OR REFRESH`** (never `CREATE OR REPLACE`) for streaming tables and
  materialized views.
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
- Catalog: existing `<catalog>` (placeholder; replace with workshop catalog name).
- Per-attendee schema: `<catalog>.<user>` (exists; setup notebook never creates/mutates it).
- Shared landing volume: `/Volumes/<catalog>/shared/landing/booking_fraud_flags/`
  — JSON fraud markers keyed by `booking_id`.

## Setup-notebook responsibilities (MUST include)
The setup notebook (run once per workshop) must:
1. Create schema `<catalog>.shared` if it does not exist.
2. Create volume `<catalog>.shared.landing` if it does not exist.
3. Create folder `booking_fraud_flags/` in the volume and seed JSON fraud markers
   keyed to real `booking_id`s from `samples.wanderbricks.booking_updates`.
4. **Grant `USE_SCHEMA` on `<catalog>.shared` to group `account users`**
   (so attendees can see the volume).
5. **Grant `READ_VOLUME` on `<catalog>.shared.landing` to group `account users`**
   (read-only — no `WRITE_VOLUME`). This makes the volume world-readable within
   the workspace so every attendee can ingest from it, while preventing any
   attendee from modifying the shared JSON files.
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
  - `catalog_name` — default is `daiwt_gourmet`; change to `<catalog>`.
  - `prod_warehouse_id` — default is an example ID that likely doesn't exist in the workshop workspace; replace with a real one from **SQL Warehouses**.
  - `schema_name` — default is `${workspace.current_user.short_name}`; leave it if that matches the attendee's `<user>` schema, otherwise override to the literal `<user>` value.
- Gotcha: dashboard SQL cannot be parameterized yet — if the attendee changes catalog/schema away from defaults, they must manually replace `daiwt_gourmet` in `resources/dashboard_gourmet_aibi.yml` and `src/aibi_dashboard.json`.
- Do NOT add a copy of the Gourmet Pipeline code to this repo. Lab 3 points to the upstream repo and students clone via Workspace **Create → Git folder** with sparse checkout path `Lakeflow-Gourmet-Pipeline`.

## Files in this repo
- `README.md` — course overview.
- `Labguide.md` — attendee-facing lab guide.
- `CLAUDE.md` — this file.
- `.gitignore` — standard Databricks/Python ignores.
- `lab1-bakehouse/` — Lab 1 reference transformation files (`sales_transactions_bronze.py`, `sales_stats.sql`).
- `lab2-wanderbricks/` — Lab 2 reference transformation files (four `.sql` files).
