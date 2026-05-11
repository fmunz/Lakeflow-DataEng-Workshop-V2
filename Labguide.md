# Lakeflow-DataEng-Workshop-V2 — Lab Guide

> **Audience**: workshop attendee with a pre-assigned schema `workshop.<user>` (e.g. `workshop.user042`).
> **Structure**: four labs.
> - **Lab 1 — Bakehouse (hand-coded)**: streaming table in **Python**, materialized view in **SQL**, plus data-quality expectations and a managed-Iceberg side-quest read with PyIceberg. Reference files in [`lab1-bakehouse/`](./lab1-bakehouse/).
> - **Lab 2 — Wanderbricks (Genie-Code-generated)**: four-file all-**SQL** pipeline (AutoCDC + Auto Loader + join gold MV), produced from a single Genie Code prompt — and verified by you before it runs. Reference files in [`lab2-wanderbricks/`](./lab2-wanderbricks/).
> - **Lab 3 — Gourmet Pipeline (DAB deploy)**: clone the public `databricks/tmm/Lakeflow-Gourmet-Pipeline` asset bundle, adjust a couple of variables so it targets your own `workshop.<user>`, and deploy + run it from the Workspace UI.
> - **Lab 4 — Zerobus (direct-to-Delta REST ingest)**: push one temperature reading into `workshop.zerobus.course_temp` via the Zerobus REST API, with credentials fetched from a shared secret scope. Reference files in [`lab4-zerobus/`](./lab4-zerobus/).
>
> Pedagogical arc: Lab 1 = author by hand. Lab 2 = author with AI, verify. Lab 3 = deploy someone else's production-style bundle.

## Prerequisites (already done by the setup notebook)

- Your schema `workshop.<user>` already exists and is writable.
- Shared volume exists at `/Volumes/workshop/shared/landing/` with a seeded subfolder `booking_fraud_flags/` containing JSON fraud markers keyed by `booking_id`. The volume is **read-only** for attendees (every attendee has `READ_VOLUME`, nobody has `WRITE_VOLUME`), so one attendee cannot disrupt another.
- Serverless compute is enabled.
- You can read `samples.bakehouse.*` and `samples.wanderbricks.*` (public sample data).
- **Partner-powered AI features** are enabled (Genie Code requires this).

---

## Lab 0 — Create the pipeline (new Lakeflow Pipelines Editor)

1. Workspace sidebar → **New** → **ETL pipeline**. The **Lakeflow Pipelines Editor** opens with a default name `New Pipeline <date> <time>`.
2. Click the name → rename to `workshop_<user>`.
3. **Right of the pipeline name**, click the catalog/schema selector — a **Default location** modal opens. Set:
   - **Default catalog**: `workshop`
   - **Default schema**: type `<user>` and click **Save**. The dropdown sometimes only offers *"Create schema"* even though your `<user>` schema already exists — ignore that, the typed literal is accepted.

   Unqualified table names now resolve to `workshop.<user>.<table>`. The full **Pipeline settings** panel may open after Save — close it with the ✕ to return to the editor.
4. The default file `my_transformation.py` is already Python (Lab 1a uses Python). The editor opens blank with a placeholder — just start typing.
5. Confirm **⚙ Settings** shows **Serverless** ON and Unity Catalog selected.

---

## Lab 1 — Bakehouse (hand-coded)

You'll ingest `samples.bakehouse.sales_transactions` (3,333 bakery transactions) and compute KPIs. One streaming table in Python, one materialized view in SQL — deliberate so you see both authoring styles in the same lab.

### Step 1a — Streaming table (Python)

Paste into `my_transformation.py`:

```python
from pyspark import pipelines as dp


@dp.table(
    name="sales_transactions",
    comment="Raw bakery transactions streamed from samples.bakehouse.sales_transactions",
)
def sales_transactions():
    # spark.readStream.table(...) inside @dp.table ⇒ streaming table
    return spark.readStream.table("samples.bakehouse.sales_transactions")
```

Click **Run file**. The DAG sidebar shows one node `sales_transactions` (~3,333 rows).

### Step 1b — Materialized view (SQL)

Asset browser → **Add → Transformation** → name `sales_stats`, language **SQL** → **Create**. Paste:

```sql
CREATE OR REFRESH MATERIALIZED VIEW sales_stats
COMMENT 'Sales KPIs grouped by product and payment method'
TBLPROPERTIES ('quality' = 'silver')
AS SELECT
    product,
    paymentMethod,
    COUNT(*)                     AS txn_count,
    SUM(quantity)                AS units_sold,
    ROUND(SUM(totalPrice), 2)    AS gross_revenue,
    ROUND(AVG(totalPrice), 2)    AS avg_txn_value,
    COUNT(DISTINCT customerID)   AS unique_customers,
    COUNT(DISTINCT franchiseID)  AS franchises_selling
FROM sales_transactions
GROUP BY product, paymentMethod;
```

Click **Run pipeline**. The DAG now shows `sales_transactions → sales_stats` (up to 6 × 3 = 18 rows, one per product × payment method).

**Key teaching points**
- Python for the streaming table (1a): `from pyspark import pipelines as dp` + `@dp.table` + `spark.readStream.table(...)` — modern SDP API. Not legacy `import dlt`.
- SQL for the materialized view (1b): `CREATE OR REFRESH MATERIALIZED VIEW` (never `CREATE OR REPLACE`). The relative name `sales_transactions` resolves against the pipeline's default catalog + schema.
- Same pipeline can mix Python and SQL files — no special configuration needed.

### Step 1c — Add data-quality expectations to `sales_stats`

SDP has **one** constraint syntax — `CONSTRAINT <name> EXPECT (<predicate>)` — and **three** violation behaviors: *log* (default), *drop row*, and *fail update*. Instead of showing three alternative MVs, wire all three into a single `sales_stats` definition — one expectation per behavior, each checking something different.

Replace the body of `sales_stats.sql` with:

```sql
CREATE OR REFRESH MATERIALIZED VIEW sales_stats (
    -- 1. LOG (default): average transaction value within a reasonable range.
    --    Violations are counted in the event log; rows are still written.
    CONSTRAINT reasonable_avg_value
        EXPECT (avg_txn_value BETWEEN 1 AND 1000),

    -- 2. DROP ROW: gross revenue must be non-negative.
    --    Violating rows are excluded from the target; pipeline continues.
    CONSTRAINT nonneg_revenue
        EXPECT (gross_revenue >= 0) ON VIOLATION DROP ROW,

    -- 3. FAIL UPDATE: payment method must be populated.
    --    Any violation aborts the whole pipeline update with the constraint name.
    CONSTRAINT known_payment_method
        EXPECT (paymentMethod IS NOT NULL) ON VIOLATION FAIL UPDATE
)
COMMENT 'Sales KPIs grouped by product and payment method, with data-quality expectations'
AS SELECT
    product,
    paymentMethod,
    COUNT(*)                     AS txn_count,
    SUM(quantity)                AS units_sold,
    ROUND(SUM(totalPrice), 2)    AS gross_revenue,
    ROUND(AVG(totalPrice), 2)    AS avg_txn_value,
    COUNT(DISTINCT customerID)   AS unique_customers,
    COUNT(DISTINCT franchiseID)  AS franchises_selling
FROM sales_transactions
GROUP BY product, paymentMethod;
```

Click **Run pipeline**. The `sales_stats` node in the DAG now shows the three constraints in its sidebar. Open the event log and filter for `flow_progress` → you'll see a `data_quality.expectations` block with all three constraint names and their `passed_records` / `failed_records` counts.

**What to observe**
- The bakehouse sample data is clean, so all three expectations pass and row counts match Step 1b.
- To see a *drop* in action, weaken one predicate (e.g., `EXPECT (avg_txn_value > 10000) ON VIOLATION DROP ROW`) and re-run — rows disappear and the dropped count climbs.
- To see an *abort*, flip `known_payment_method` to `EXPECT (paymentMethod = 'gold_card')` — the update fails with the constraint name in the error.
- Same `CONSTRAINT ... EXPECT ... [ON VIOLATION ...]` syntax works on **streaming tables**: put the block inside `CREATE OR REFRESH STREAMING TABLE <name> (...)`.

### Step 1d — Top-5 sales locations as a managed Iceberg table (SQL, outside the pipeline)

The streaming table is Delta. To make a derived result readable by any Iceberg-compatible engine (PyIceberg, Trino, Snowflake, OSS Spark) without configuring an external location for UniForm Compatibility Mode, the simplest path is a **CTAS into a managed Iceberg table** — run it *outside* the pipeline in the SQL editor or a `%sql` cell.

Open a new SQL editor tab (sidebar **SQL Editor** → **Create new query**) and run, replacing `workshop` and `<user>`:

```sql
CREATE OR REPLACE TABLE workshop.<user>.global_sales_gold
USING ICEBERG
AS
SELECT
    f.city,
    f.country,
    COUNT(*)                    AS txn_count,
    SUM(t.quantity)             AS units_sold,
    ROUND(SUM(t.totalPrice), 2) AS gross_revenue
FROM workshop.<user>.sales_transactions t
JOIN samples.bakehouse.sales_franchises f
    USING (franchiseID)
GROUP BY f.city, f.country
ORDER BY gross_revenue DESC
LIMIT 5;
```

**Why this shape:**
- `USING ICEBERG` creates a **native managed Iceberg table** — full read/write from Databricks *and* external engines via the UC Iceberg REST Catalog (IRC).
- No `delta.universalFormat.*` properties, no `compatibility.location`, no external location to pre-configure. Managed Iceberg is self-contained.
- Snapshot, not live. Re-run this CTAS (or swap to `INSERT OVERWRITE`) to refresh — acceptable for an analytics/gold table; if you need live-as-it-changes semantics, that's UniForm Compatibility Mode territory instead.
- `samples.bakehouse.sales_franchises` supplies the `city` / `country` dimensions — `sales_transactions` itself only has `franchiseID`.

Verify:

```sql
SELECT * FROM workshop.<user>.global_sales_gold;
```

You should see 5 rows, top cities by gross revenue. In `DESCRIBE EXTENDED`, the `Provider` column reads `iceberg`.

### Step 1e — Read the Iceberg table with PyIceberg (Exploration notebook)

Now read the same table with a lightweight Iceberg client — no Spark required.

Asset browser → **Add → Exploration** → name `read_global_sales_gold` → language **Python** → **Create**. Paste:

```python
# MAGIC %pip install --upgrade "pyiceberg>=0.9,<0.10" "pyarrow>=17,<20"
# (Azure workspaces only) %pip install adlfs
dbutils.library.restartPython()
```

```python
from pyiceberg.catalog import load_catalog

WORKSPACE = spark.conf.get("spark.databricks.workspaceUrl")  # e.g. "dbc-xxx.cloud.databricks.com"
CATALOG   = "workshop"   # workshop UC catalog
SCHEMA    = "<user>"      # your pre-assigned schema
TOKEN     = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

iceberg_catalog = load_catalog(
    "uc",
    uri=f"https://{WORKSPACE}/api/2.1/unity-catalog/iceberg-rest",
    warehouse=CATALOG,     # pins the UC catalog — subsequent identifiers are <schema>.<table>
    token=TOKEN,
)

tbl = iceberg_catalog.load_table(f"{SCHEMA}.global_sales_gold")
print(tbl.current_snapshot())             # snapshot metadata proves this is Iceberg
tbl.scan(limit=10).to_pandas()            # top-5 rows as pandas
```

**What you just demonstrated:**
- `pip install pyiceberg` — no cluster restart, no Iceberg JARs, no Spark session. A pure-Python client talks to Unity Catalog via the **Iceberg REST Catalog** endpoint at `/api/2.1/unity-catalog/iceberg-rest`.
- The `warehouse` parameter pins the UC catalog, so table identifiers collapse from three-part to two-part.
- For external clients (outside Databricks), the same code works — just supply a PAT or OAuth token and the workspace URL. That's the portability story for Iceberg on UC.

**Requirements your admin has likely already set** (workshop attendees usually inherit these; flag with the instructor if any call fails with `403`):
- `EXTERNAL USE SCHEMA` on `workshop.<user>`
- External data access enabled on the workspace
- Workspace IP access list (if enabled) allows your client

> Reference copies of the CTAS and the PyIceberg reader live in [`lab1-bakehouse/`](./lab1-bakehouse/) alongside this guide.

---

## Lab 2 — Wanderbricks in SQL (generated by Genie Code)

In this lab you do not write SQL yourself. You give Genie Code one prompt; it proposes a plan and four SQL files; you **verify** each file before approving it to run.

### Open Genie Code

1. Upper-right of the workspace → click **Genie Code**. The side panel opens.
2. Lower-right of the panel → confirm the **Agent** toggle is on.
3. Expect approval prompts (Allow / Decline / Allow in this thread / Always allow) whenever Genie Code wants to create a file or run code — **never** click *Always allow* in this lab; reviewing each diff is the point.

### The prompt

Paste the following into Genie Code Agent:

```text
In this Lakeflow Pipelines Editor, build a SQL-only pipeline that answers:
"For our Wanderbricks bookings, how many are fraudulent and how much gross revenue is at risk, broken down by payment method?"

Inputs:
- samples.wanderbricks.booking_updates — a CDC stream of booking-state changes (natural key booking_id, sequence column updated_at).
- samples.wanderbricks.payments — one or more payment rows per booking with amount and payment_method.
- /Volumes/workshop/shared/landing/booking_fraud_flags/ — JSON files marking fraudulent bookings (fields: booking_id, flag, reason, flagged_at, confidence).

Please:
1. Ingest the two sample tables and the JSON volume with the appropriate SDP pattern for each (AutoCDC, Auto Loader, plain stream).
2. Produce one gold materialized view aggregated by payment_method showing booking_count, gross_amount, fraud_count, fraud_amount, fraud_pct.
3. Use CREATE OR REFRESH throughout. Run the pipeline and report row counts.
```

This is deliberately higher-level — it states the *business question* and the *inputs*, and lets Genie Code plan the solution (table names, columns, join shape, aggregation form). This is the honest way to use an AI data engineering agent.

### Verify — the step that matters most

Because the prompt is high-level, Genie Code has room to make choices. Before clicking **Allow** on each proposed file, check it against the reference SQL below. Expected properties of a good generation:

- Four files: a streaming table with `AUTO CDC INTO` on `booking_updates`, a streaming table using `STREAM read_files(...)` on the JSON volume, a plain streaming table on `payments`, and one materialized view.
- `CREATE OR REFRESH` everywhere — never `CREATE OR REPLACE`.
- `SCD TYPE 1` for the AutoCDC target (TYPE 2 is also acceptable but reviewed for your use case).
- The MV groups by `payment_method` and includes `fraud_count` / `fraud_amount` / `fraud_pct`.

If a file drifts (extra staging tables, `CREATE OR REPLACE`, missing `STREAM` keyword on `read_files`, a Python file instead of SQL), **Decline** and ask Genie Code to fix it, e.g. *"Replace CREATE OR REPLACE with CREATE OR REFRESH"* or *"Remove the intermediate table — keep only the four files"*.

The reference SQL below is **one valid shape** — your generation may use different table names or column names. That is fine as long as the result answers the business question.

#### Reference — `bookings_current.sql`

```sql
CREATE OR REFRESH STREAMING TABLE bookings_current
COMMENT 'Latest state of each booking, rebuilt from booking_updates via AutoCDC';

CREATE FLOW bookings_current_flow AS AUTO CDC INTO bookings_current
FROM STREAM samples.wanderbricks.booking_updates
KEYS (booking_id)
SEQUENCE BY updated_at
COLUMNS * EXCEPT (booking_update_id)
STORED AS SCD TYPE 1;
```

Expected after run: one row per distinct `booking_id` in `booking_updates` (~48K). Booking `50928` should have status `completed`.

#### Reference — `booking_fraud_flags.sql`

```sql
CREATE OR REFRESH STREAMING TABLE booking_fraud_flags
COMMENT 'Fraud markers for bookings, ingested via Auto Loader from the shared landing volume'
AS SELECT
    booking_id,
    flag,
    reason,
    CAST(flagged_at AS TIMESTAMP)    AS flagged_at,
    confidence,
    _metadata.file_path              AS source_file,
    _metadata.file_modification_time AS source_file_ts
FROM STREAM read_files(
    '/Volumes/workshop/shared/landing/booking_fraud_flags/',
    format => 'json'
);
```

Expected after run: row count equals the number of JSON records seeded in the shared volume; `source_file` populated.

#### Reference — `payments.sql`

```sql
CREATE OR REFRESH STREAMING TABLE payments
COMMENT 'Payments stream from samples.wanderbricks.payments'
AS SELECT
    payment_id,
    booking_id,
    amount,
    payment_method,
    status,
    payment_date
FROM STREAM samples.wanderbricks.payments;
```

Expected after run: ~49,638 rows.

#### Reference — `booking_fraud_summary.sql`

```sql
CREATE OR REFRESH MATERIALIZED VIEW booking_fraud_summary
COMMENT 'Booking totals and fraud rate per payment method'
TBLPROPERTIES ('quality' = 'gold')
AS
WITH fraud AS (
    SELECT DISTINCT booking_id
    FROM booking_fraud_flags
    WHERE flag = 'fraud'
)
SELECT
    p.payment_method,
    COUNT(*)                                                                   AS booking_count,
    ROUND(SUM(p.amount), 2)                                                    AS gross_amount,
    COUNT(f.booking_id)                                                        AS fraud_count,
    ROUND(SUM(CASE WHEN f.booking_id IS NOT NULL THEN p.amount ELSE 0 END), 2) AS fraud_amount,
    ROUND(COUNT(f.booking_id) * 100.0 / COUNT(*), 2)                           AS fraud_pct
FROM bookings_current b
JOIN payments  p ON p.booking_id = b.booking_id
LEFT JOIN fraud       f ON f.booking_id = b.booking_id
GROUP BY p.payment_method;
```

Expected after run: 5 rows (one per `payment_method`: credit_card, paypal, apple_pay, google_pay, bank_transfer). Each should have non-zero `fraud_pct` and `fraud_amount` in the same units as `payments.amount`.

### Final Lab 2 DAG

```
booking_updates        ─► bookings_current    ─┐
                                                │
samples.payments       ─► payments             ─┤
                                                ├─► booking_fraud_summary
booking_fraud_flags    ─► booking_fraud_flags  ─┘
(JSON volume)
```

### Optional challenge — ask Genie Code in Chat mode

Switch Genie Code to **Chat** mode and ask:

```text
Explain the data flow in this pipeline end-to-end. Which node is incrementally maintained versus fully recomputed on refresh, and why?
```

> Reference copies of the four SQL files live in [`lab2-wanderbricks/`](./lab2-wanderbricks/) alongside this guide — use them as the answer key when verifying Genie Code's output.

---

## Lab 3 — Deploy the Gourmet Pipeline (Databricks Asset Bundle)

Labs 1–2 had you author files inside the pipeline editor. Lab 3 is a production-style workflow: **clone** an existing open-source Databricks Asset Bundle (DAB) from GitHub, **adjust two variables** so it targets your own schema, and **deploy + run** it from the Workspace UI.

Source repo: `https://github.com/databricks/tmm/tree/main/Lakeflow-Gourmet-Pipeline`.

### What Gourmet Pipeline demonstrates

A global snack company's end-to-end data product: a Bronze → Silver → Gold medallion architecture authored entirely in **SQL** with Spark Declarative Pipelines, ingestion via **Lakeflow Connect** (from SFDC, MS SQL, and XML volumes — the workshop version uses stub endpoints so you don't need real creds), enrichment with **AI functions** (recipe generation, sentiment, translation), and a published **AI/BI dashboard** served via **Databricks One**. A single `gourmet-workflow` job orchestrates the whole thing. Data-quality rules are declared inline with `EXPECT` constraints.

### Step 3a — Clone the repo into your workspace

1. Workspace sidebar → click **Workspace**.
2. Top-right **Create** → **Git folder**.
3. In the **Create Git folder** dialog:
   - **Git repository URL**: `https://github.com/databricks/tmm`
   - **Git provider**: GitHub
   - Enable **Sparse checkout mode**
   - **Sparse checkout path**: `Lakeflow-Gourmet-Pipeline`
4. Click **Create Git folder**. The subfolder clones into your workspace.

### Step 3b — Adjust `databricks.yml` for your schema

Open `Lakeflow-Gourmet-Pipeline/databricks.yml`. The `variables` block you need to edit looks like this (defaults shown):

```yaml
variables:
  prod_warehouse_id:
    description: define prod warehouse id
    default: 5834a7035a11c525       # ← replace with a warehouse in YOUR workspace

  catalog_name:
    description: "Catalog name for the pipeline"
    default: daiwt_gourmet          # ← change to your workshop catalog: workshop

  schema_name:
    description: "Schema name for the pipeline"
    default: ${workspace.current_user.short_name}
                                    # ← see note below: usually leave this alone,
                                    #   override to <user> only if needed
```

**Per-student adjustments (do this in the file):**

1. **`catalog_name`** — change the `default:` value to the workshop catalog (replace `workshop` with the name your instructor gave you).
2. **`prod_warehouse_id`** — go to the workspace sidebar → **SQL Warehouses**, copy the ID of a running warehouse, and paste it here.
3. **`schema_name`** — the default `${workspace.current_user.short_name}` automatically resolves to your Databricks username short form. **If that already matches your pre-assigned `<user>` schema** (common when the workshop schema name = your username), leave it. **If your workshop schema uses a different naming convention**, override the `default:` to your literal `<user>` value.

Also check the `targets.presenter` block at the bottom — if it overrides `schema_name`, make sure it also points at `<user>`.

### Step 3c — Verify the AI model endpoint exists on your workspace

The `new_recipe_Claude_LLM` task in `gourmet-workflow` calls `ai_query('databricks-claude-3-7-sonnet', ...)` from `src/ai_query.sql`. That older endpoint is **not** available on every workspace. Check before you deploy:

1. Workspace sidebar → **Serving**. Look for `databricks-claude-3-7-sonnet` in the endpoints list.
2. **If present** — do nothing, skip to Step 3d.
3. **If missing** — pick a Claude endpoint that IS available (e.g. `databricks-claude-sonnet-4-5` or `databricks-claude-haiku-4-5`) and edit `src/ai_query.sql`, replacing **both** occurrences of `databricks-claude-3-7-sonnet` (one in the `COMMENT` docstring, one in the `ai_query(...)` call).

Alternatively, if you don't want to run the AI tasks at all, skip this step and later at Step 3e override the `ai_enabled` job parameter to `FALSE` when you trigger the run — the workflow will short-circuit after the ETL pipeline.

### Step 3d — Deploy the bundle

1. Navigate into the cloned `Lakeflow-Gourmet-Pipeline/` folder in Workspace. Because `databricks.yml` is present, the left pane shows the **Deployments** icon (🚀).
2. Click **Deployments** → select the target workspace (the bundle defaults to a target named `presenter`) → click **Deploy**.
3. Wait for validate + deploy to finish. The **Bundle resources** panel populates with the deployed assets: SDP pipelines (bronze/silver/gold transformations), a multi-task workflow `gourmet-workflow`, and an AI/BI dashboard.

### Step 3e — Run the workflow

1. In **Bundle resources**, find **Jobs → `gourmet-workflow`**. Click the **Run** (▶) icon.
2. Monitor progress in **Job Runs**. The workflow ingests franchise / supplier / transaction data, runs the SDP transformations, executes the AI enrichment steps, and refreshes the dashboard.
3. After the run finishes, open the deployed **AI/BI dashboard** and explore it — it blends real-time transaction data with AI-generated localized marketing campaigns.

### Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| Bundle validate fails on warehouse ID | default `prod_warehouse_id` doesn't exist in your workspace | paste a real warehouse ID from **SQL Warehouses** |
| `new_recipe_Claude_LLM` task fails with `RESOURCE_DOES_NOT_EXIST` for endpoint `databricks-claude-3-7-sonnet` | you skipped Step 3c — the hardcoded model isn't on this workspace | go back to Step 3c: either edit `src/ai_query.sql` to a Claude endpoint that exists on your workspace, or rerun with job parameter `ai_enabled=FALSE` |
| SCHEMA_NOT_FOUND errors during run | `schema_name` resolves to something that doesn't exist in your catalog | override the `schema_name` default to your literal `<user>` |
| SFDC / MS SQL connection errors | shouldn't happen — the demo uses stub endpoints | re-check you're on the demo branch of the repo, not a modified fork |

### What to take away

- **Bundles as deploy unit**: `databricks.yml` + `resources/*.yml` captures an entire data product (SDP pipelines + jobs + dashboards + Lakeflow Connect flows) as version-controlled code.
- **Variables + targets**: `${workspace.current_user.short_name}` lets one bundle deploy per-user in a workshop with minimal per-student edits.
- **Git folder + sparse checkout**: clone exactly the subfolder you need from a large repo without pulling everything.
- **Workspace-native deploy**: the **Deployments** (🚀) pane is an alternative to `databricks bundle deploy` from the CLI when attendees don't have the CLI installed.

---

## Lab 4 — Send a temperature reading via Zerobus (REST, serverless-friendly)

So far every table you've built has ingested from batch / streaming **sources that already exist** (sample tables, a JSON volume, Delta tables). Lab 4 flips that around: **you** are the producer. One HTTP POST per reading writes directly into a Delta table — no Kafka, no Auto Loader, no pipeline. That's **Zerobus Ingest**.

Zerobus offers three interfaces (gRPC SDK, REST, OpenTelemetry). The SDK is best for high-throughput producers but can't `pip install` on serverless — so for this workshop you'll use the **REST API** (Beta), which is just `requests.post`. Perfect for "chatty" low-frequency producers like this one.

### Target

A managed Delta table provisioned by the setup notebook:

| catalog | schema | table | columns |
|---|---|---|---|
| `workshop` | `zerobus` | `course_temp` | `id STRING, city STRING, temp FLOAT` |

### Credentials model

Your notebook never sees the service principal credentials. The setup notebook created:

- One shared SP `workshop-zerobus-sp` with `MODIFY + SELECT` only on `workshop.zerobus.course_temp` (nothing else)
- A secret scope named `workshop` holding `zerobus_client_id`, `zerobus_client_secret`, `zerobus_endpoint`, `zerobus_workspace_id`, `zerobus_workspace_url`
- `READ` ACL on the scope granted to `account users`

At runtime your notebook reads the five values via `dbutils.secrets.get("workshop", ...)`. If someone leaks the client_secret, its blast radius is the single table it can write to.

### Step 4a — Open the reference notebook

Asset browser (or Workspace) → **Add → Exploration** → name `send_temperature` → language **Python** → **Create**. Paste the contents of [`lab4-zerobus/send_temperature.py`](./lab4-zerobus/send_temperature.py).

### Step 4b — Fill in the two widgets at the top

- **City** — your city (e.g. `Munich`)
- **Temperature (°C)** — any float (e.g. `21.5`)

### Step 4c — Run the **Submit** cell

That cell calls `submit_temperature(CITY, TEMP)`, which the notebook's plumbing cell defines. The plumbing cell is marked **⛔ DO NOT MODIFY** — it reads secrets, generates a UUID, exchanges the SP creds for an OAuth token scoped to the table via `authorization_details`, and posts a one-element JSON array to:

```
POST https://<workspace_id>.zerobus.<region>.cloud.databricks.com/zerobus/v1/tables/workshop.zerobus.course_temp/insert
```

On success you'll see:

```
✅ Sent to workshop.zerobus.course_temp: {'id': '…', 'city': 'Munich', 'temp': 21.5}
```

### Step 4d — Verify

The last cell runs `spark.table("workshop.zerobus.course_temp").where("city = 'Munich'")`. Your row appears within a few seconds (Zerobus is at-least-once; order isn't guaranteed across producers).

### What to take away

- **Direct-to-Delta ingest** — one POST, one row, no intermediate bus. Ideal for edge devices or low-volume producers.
- **Fine-grained OAuth** — the `authorization_details` param scopes the token to a single table, not the whole workspace. A leaked token can only write to `course_temp`.
- **Secret scope, not copy-paste** — attendees read creds from a scope, they're never in the notebook source. Re-running the setup rotates the secret with no change required on attendee side.
- **REST vs gRPC SDK trade-off** — REST is a handshake per record (higher per-record cost), gRPC holds a persistent stream (much higher throughput). For this workshop, one record per attendee, REST is the right call. At volume, use the SDK.
- **Zerobus does not create tables** — the table must exist with the exact schema before any record can land.

> Reference notebook: [`lab4-zerobus/send_temperature.py`](./lab4-zerobus/send_temperature.py).

---

## Wrap-up

You now have seven tables across Labs 1-2-4, a full deployed bundle from Lab 3, and an Iceberg side-quest:

| Lab | Table | Type | Source | Language |
|---|---|---|---|---|
| 1 | `sales_transactions` | Streaming Table | `samples.bakehouse.sales_transactions` | Python |
| 1 | `sales_stats` | Materialized View | `sales_transactions` | SQL |
| 1 | `global_sales_gold` | Managed Iceberg table (CTAS, outside pipeline) | `sales_transactions` ⨝ `samples.bakehouse.sales_franchises` | SQL |
| 2 | `bookings_current` | Streaming Table + AutoCDC | `samples.wanderbricks.booking_updates` | SQL |
| 2 | `booking_fraud_flags` | Streaming Table + Auto Loader | `/Volumes/workshop/shared/landing/booking_fraud_flags/` | SQL |
| 2 | `payments` | Streaming Table | `samples.wanderbricks.payments` | SQL |
| 2 | `booking_fraud_summary` | Materialized View | `bookings_current` ⨝ `payments` ⨝ `booking_fraud_flags` | SQL |
| 3 | *(bundle)* | SDP pipelines + workflow + AI/BI dashboard deployed from `databricks/tmm/Lakeflow-Gourmet-Pipeline` | `workshop.<user>` | SQL |
| 4 | `workshop.zerobus.course_temp` | Managed Delta table (shared), written via Zerobus REST | HTTP POST from attendee notebook | Python |

### UI cheat sheet

| Action | Where |
|---|---|
| Create pipeline | Sidebar **New → ETL pipeline** |
| Set default catalog/schema | Selector right of pipeline name |
| Add a transformation file | Asset browser **Add → Transformation** |
| Add a non-pipeline notebook | Asset browser **Add → Exploration** |
| Run current file only | **Run file** |
| Run the whole pipeline | **Run pipeline** |
| Schedule as a job | Top bar **Schedule** |
| **Open Genie Code** | **Genie Code** button, upper-right of workspace |
| **Toggle Agent mode** | **Agent** selector, lower-right of Genie Code panel |
| Clone a Git folder | Workspace sidebar **Create → Git folder** (sparse checkout supported) |
| Deploy a bundle from UI | Open the bundle folder → **Deployments** icon (🚀) in the left pane |
