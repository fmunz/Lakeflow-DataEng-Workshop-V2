# Lakeflow-DataEng-Workshop-V2 — Lab Guide

> **Audience**: workshop attendee with a pre-assigned schema `workshop.<user>` (e.g. `workshop.user042`).
> **Structure**: four core labs plus one optional side-quest.
> - **Lab 1 — Bakehouse (hand-coded)**: streaming table in **Python**, materialized view in **SQL** with three data-quality expectations wired in from the start. Reference files in [`lab1-bakehouse/`](./lab1-bakehouse/).
> - **Lab 2 — Learn how to use Genie Code (Wanderbricks)**: four-file all-**SQL** pipeline (AutoCDC + Auto Loader + join gold MV), produced from a single Genie Code prompt — and verified by you before it runs. Reference files in [`lab2-wanderbricks/`](./lab2-wanderbricks/).
> - **Lab 3 — CI/CD via Declarative Automation Bundles**: clone the public `databricks/tmm/Lakeflow-Gourmet-Pipeline` bundle, retarget two variables to `workshop.<user>`, and deploy it from the Workspace UI — the same bundle a CI runner would ship with `databricks bundle deploy`.
> - **Lab 4 — Push IoT temperature reading via Zerobus Ingest** *(live instructor demo; attendees may follow along)*: one HTTP POST lands a row in `workshop.zerobus.course_temp`, with credentials fetched from a shared secret scope. Reference files in [`lab4-zerobus/`](./lab4-zerobus/).
> - **Lab 5 — Iceberg side-quest** *(optional)*: publish a derived bakehouse result as a managed Iceberg table and read it back with PyIceberg through the Unity Catalog Iceberg REST endpoint — no Spark session required. Reference files in [`lab5-iceberg/`](./lab5-iceberg/).
>
> Pedagogical arc: Lab 1 = author by hand. Lab 2 = author with AI, verify. Lab 3 = deploy someone else's bundle — the CI/CD primitive. Lab 4 = produce from outside the platform. Lab 5 = take-home bonus on Iceberg interoperability.
>
> SDP's pitch is simple: you declare the target table; the platform owns the scheduling, dependencies, and incremental state.

## Prerequisites (already done by the setup notebook)

- Your schema `workshop.<user>` already exists and is writable.
- Shared volume exists at `/Volumes/workshop/shared/landing/` with a seeded subfolder `booking_fraud_flags/` containing JSON fraud markers keyed by `booking_id`. The volume is **read-only** for attendees (every attendee has `READ_VOLUME`, nobody has `WRITE_VOLUME`), so one attendee cannot disrupt another.
- Serverless compute is enabled.
- You can read `samples.bakehouse.*` and `samples.wanderbricks.*` (public sample data).
- **Partner-powered AI features** are enabled (Genie Code requires this).

### Substitutions

Three placeholders show up throughout — resolve them once here, then paste blocks run as-is.

| Placeholder | What to use |
|---|---|
| `<user>` | Your pre-assigned schema name (e.g. `user042`). Find it in Catalog Explorer under `workshop`. |
| `workshop` | The catalog. Fixed — do not change. |
| `prod_warehouse_id` (Lab 3 only) | A running SQL warehouse ID. Find it in sidebar **SQL Warehouses** → click a warehouse → copy the ID from the URL. |
| `<course_warehouse_name>` / `<course_warehouse_id>` (Lab 4 only) | The course SQL warehouse provisioned for you by the courseware. Your instructor will share the exact name and ID. |

---

## Lab 1 — Bakehouse (hand-coded)

You'll hand-code a Spark Declarative Pipeline over the bakehouse sample data: one Streaming Table in Python for incremental ingest, and one Materialized View in SQL for KPIs with three `EXPECT` expectations wired in from the start so you see log / drop / fail behaviors in a single round trip.

### Set up the pipeline in the Lakeflow Pipelines Editor

Before you write a single line, create the pipeline that will host Steps 1a and 1b:

1. Workspace sidebar → **New** → **ETL pipeline**. The **Lakeflow Pipelines Editor** opens with a default name `New Pipeline <date> <time>`.
2. Click the name → rename to `workshop_<user>`.
3. **Right of the pipeline name**, click the catalog/schema selector — a **Default location** modal opens. Set:
   - **Default catalog**: `workshop`
   - **Default schema**: type `<user>` and click **Save**. The dropdown sometimes only offers *"Create schema"* even though your `<user>` schema already exists — ignore that, the typed literal is accepted.

   Unqualified table names now resolve to `workshop.<user>.<table>`. The full **Pipeline settings** panel may open after Save — close it with the ✕ to return to the editor.
4. The default file `my_transformation.py` is already Python — Step 1a uses Python. The editor opens blank with a placeholder; just start typing.
5. Confirm **⚙ Settings** shows **Serverless** ON and Unity Catalog selected.

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

### Step 1b — Materialized view with data-quality expectations (SQL, copy-and-paste)

Asset browser → **Add → Transformation** → name `sales_stats`, language **SQL** → **Create**. Paste the block below — no replacement step later; the three expectations are already wired in:

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

Click **Run pipeline**. The DAG now shows `sales_transactions → sales_stats` (up to 6 × 3 = 18 rows, one per product × payment method). The `sales_stats` node shows the three constraints in its sidebar. Open the event log and filter for `flow_progress` → you'll see a `data_quality.expectations` block with each constraint's `passed_records` / `failed_records` counts.

SDP has **one** constraint syntax — `CONSTRAINT <name> EXPECT (<predicate>)` — and **three** violation behaviors: *log* (default), *drop row*, and *fail update*. Wiring all three into one view shows every behavior in a single round trip.

**Key teaching points**
- Python for the streaming table (1a): `from pyspark import pipelines as dp` + `@dp.table` + `spark.readStream.table(...)` — modern SDP API. Not legacy `import dlt`.
- SQL for the materialized view (1b): `CREATE OR REFRESH MATERIALIZED VIEW` (never `CREATE OR REPLACE`). The relative name `sales_transactions` resolves against the pipeline's default catalog + schema.
- Same pipeline can mix Python and SQL files — no special configuration needed.
- The bakehouse sample data is clean, so all three expectations pass and row counts match a constraint-free version.
- Same `CONSTRAINT ... EXPECT ... [ON VIOLATION ...]` syntax works on **streaming tables**: put the block inside `CREATE OR REFRESH STREAMING TABLE <name> (...)`.

**Is this MV incrementally maintained or fully recomputed?**

A Materialized View is either *incrementally maintained* (only the rows that changed are reprocessed) or *fully recomputed* on refresh, depending on whether the SDP planner can rewrite the query as an incremental update. Simple projections, filters, and many aggregations qualify for incremental maintenance; `COUNT(DISTINCT …)` — which `sales_stats` uses twice — typically forces a **COMPLETE refresh** because distinct tracking isn't incrementally maintainable without a lot more state.

Where to see which mode ran:
- **DAG node** — click the `sales_stats` node; the right-hand flow details panel shows the refresh type (`COMPLETE` vs `INCREMENTAL`) from the most recent run.
- **Event log** — filter by `event_type = 'flow_progress'` and inspect the `details.flow_progress.status` / `planning_information` fields. The planner records the chosen execution mode and, for full recomputes, the reason it couldn't go incremental.
- **Proof by experiment** — drop the two `COUNT(DISTINCT …)` expressions from `sales_stats` and re-run. The planner can now maintain the view incrementally, and the flow details flip to `INCREMENTAL`. Attendees often find this more convincing than reading docs.

**Try a violation yourself (optional)**
- To see a *drop* in action, weaken one predicate (e.g., `EXPECT (avg_txn_value > 10000) ON VIOLATION DROP ROW`) and re-run — rows disappear and the dropped count climbs.
- To see an *abort*, flip `known_payment_method` to `EXPECT (paymentMethod = 'gold_card')` — the update fails with the constraint name in the error.

### Lab 1 take-away

Streaming ingest + MV refresh + three DQ behaviors, all in about 40 lines of declarative code. Without SDP, that's a streaming job, a batch job, and a scheduler — three separate systems to wire together.

---

## Lab 2 — Learn how to use Genie Code (Wanderbricks)

Same editor, same pipeline shape, but you don't write the SQL — Genie Code does, from one business-level prompt, and you verify each of the four proposed files before approving it. Covers AutoCDC against the `booking_updates` CDC feed keyed on `booking_id`, Auto Loader over a JSON volume of fraud markers, a plain Streaming Table on payments, and a gold Materialized View that joins all three. The pedagogy is the verification loop, not the typing.

You won't write SQL in this lab. You'll prompt, you'll review, you'll approve. Genie Code drafts four files; you verify each one before it runs.

### Open Genie Code

1. Upper-right of the workspace → click **Genie Code**. The side panel opens.
2. At the bottom of the Genie Code pane, confirm the **Agent** mode selector is set to **Agent** (not **Chat**).
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

AutoCDC is a time-lapse, not a scrapbook. Every update collapses into one current row per `booking_id` — the latest state wins, history fades.

#### Reference — `bookings_current.sql`

```sql
CREATE OR REFRESH STREAMING TABLE bookings_current
COMMENT 'Latest state of each booking, rebuilt from booking_updates via AutoCDC';

CREATE FLOW bookings_current_flow AS AUTO CDC INTO bookings_current
FROM stream(samples.wanderbricks.booking_updates)
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
FROM stream(samples.wanderbricks.payments);
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

## Lab 3 — CI/CD via Declarative Automation Bundles (Gourmet Pipeline)

Lab 3 is about CI/CD for data products. A **Declarative Automation Bundle** (DAB — formerly *Databricks Asset Bundle*; the CLI is still `databricks bundle`) is the deployable unit: one `databricks.yml` plus a `resources/` folder capture an entire data product — SDP pipelines, jobs, dashboards, Lakeflow Connect flows — as versioned code. The same bundle deploys interactively from the Workspace **Deployments** pane (what you'll do here) or non-interactively from `databricks bundle deploy -t prod` in a GitHub Action. No shell recipes, no drift between envs, no screenshot-driven promotion.

You'll sparse-clone the public `databricks/tmm/Lakeflow-Gourmet-Pipeline` bundle, retarget two variables in `databricks.yml` so it points at your own schema, then deploy it. One versioned artifact lands a SQL-only Bronze-Silver-Gold SDP, a `gourmet-workflow` job with `ai_query` enrichment, and an AI/BI dashboard.

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
3. After the run finishes, open the deployed **AI/BI dashboard**.

One YAML file just deployed the medallion pipeline, the orchestration job, the AI enrichment, and the dashboard.

### The CI/CD equivalent in one snippet

You deployed interactively. A CI runner deploys the same bundle with two CLI calls:

```bash
databricks bundle validate
databricks bundle deploy -t prod
```

In a GitHub Actions workflow, that's a single step on push-to-main; `-t dev` on pull-request-open uses the same `databricks.yml` with different variable values per target. The bundle is the deployable atom; the UI and the CLI are two entry points to the same deploy.

### Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| Bundle validate fails on warehouse ID | default `prod_warehouse_id` doesn't exist in your workspace | paste a real warehouse ID from **SQL Warehouses** |
| `new_recipe_Claude_LLM` task fails with `RESOURCE_DOES_NOT_EXIST` for endpoint `databricks-claude-3-7-sonnet` | you skipped Step 3c — the hardcoded model isn't on this workspace | go back to Step 3c: either edit `src/ai_query.sql` to a Claude endpoint that exists on your workspace, or rerun with job parameter `ai_enabled=FALSE` |
| SCHEMA_NOT_FOUND errors during run | `schema_name` resolves to something that doesn't exist in your catalog | override the `schema_name` default to your literal `<user>` |
| SFDC / MS SQL connection errors | shouldn't happen — the demo uses stub endpoints | re-check you're on the demo branch of the repo, not a modified fork |

### What to take away

- **Bundles are the CI/CD primitive for data products**: `databricks.yml` + `resources/*.yml` captures an entire data product (SDP pipelines, jobs, dashboards, Lakeflow Connect flows) as versioned code. One file tree, committable, reviewable, deployable.
- **Targets separate dev from prod**: `targets:` in `databricks.yml` defines per-environment overrides. A CI pipeline runs `databricks bundle deploy -t prod` on merge to main and `-t dev` on PR open — same bundle, different catalog and warehouse.
- **Variables + workspace placeholders**: `${workspace.current_user.short_name}` lets one bundle deploy per-user in dev with no hardcoded schemas.
- **Git folder + sparse checkout**: clone exactly the subfolder you need from a large repo without pulling everything.
- **UI deploy and CLI deploy share the same bundle**: the **Deployments** (🚀) pane and `databricks bundle deploy` read the same `databricks.yml`. Hands-on-UI for learning and one-off promotions, CLI-in-CI for production.

---

## Lab 4 — Push IoT temperature reading via Zerobus Ingest

> **Format: live instructor demo.** The instructor will run this end-to-end on the projector. Attendees are welcome to follow along in their own workspace — every asset is already provisioned for you — but the teaching point is the *governance surface* (scoped OAuth, SP audit identity, secret scope), which lands better when talked through than typed in silence. If you're short on time, watch and ask questions; come back to it later.

Until now, data came to you. Sample tables, a JSON volume, a Delta stream — three ready-made sources. Lab 4 flips the script: **you** are the producer.

One HTTP POST via the Zerobus REST API writes one row directly into `workshop.zerobus.course_temp` — no Kafka, no Auto Loader, no pipeline. A shared service principal plus scoped OAuth via `authorization_details` pin the token to `MODIFY` on that single table; secrets come from a shared scope. The lab is about the governance surface an external IoT device or microservice would inherit.

Zerobus offers three interfaces (gRPC SDK, REST, OpenTelemetry). The SDK is best for high-throughput producers but can't `pip install` on serverless — so for this workshop you'll use the **REST API** (Beta), which is just `requests.post`. Perfect for "chatty" low-frequency producers like this one.

### Why Zerobus when `INSERT` is right there?

Fair question. From inside a Databricks notebook, attendees have a Spark session and could run `INSERT INTO workshop.zerobus.course_temp VALUES(...)` in two lines — if we granted them `MODIFY` on the table. We deliberately don't. Zerobus isn't built for notebooks with a Spark session. It's built for everything without one — IoT devices, microservices, edge gateways. The attendee notebook stands in for one of those external producers. What you're actually learning here — scoped OAuth via `authorization_details`, a POST to a Databricks-managed gRPC/REST gateway, a per-record ACK — is the exact code an external system would run. Running it from inside Databricks is the teaching compromise; the pattern is for outside.

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

### Step 4d — Verify in the notebook

The last cell runs `spark.table("workshop.zerobus.course_temp").where("city = 'Munich'")`. Your row appears within a few seconds (Zerobus is at-least-once; order isn't guaranteed across producers).

### Step 4e — Verify in Databricks SQL

The notebook read the table as a producer; now read it as a consumer. This proves the row is a real row in a real governed table, queryable by anything that can talk to a SQL warehouse — a BI dashboard, a downstream pipeline, a JDBC client, `ai_query(...)`.

1. Workspace sidebar → **SQL Editor** → **New query**.
2. In the top-right warehouse picker, select the **course warehouse** — `<course_warehouse_name>` (ID `<course_warehouse_id>`). It was provisioned for you by the courseware, so it's already running; you don't need to start a warehouse of your own.
3. Paste and run:

```sql
SELECT id, city, temp
FROM workshop.zerobus.course_temp
ORDER BY city, temp;
```

You should see every attendee's row, including your own. In a real production deployment this is the query a dashboard would run, refreshed on a schedule — same table, same grants, no separate serving tier.

### Governance surface — who wrote what, and what could they write?

This is the part of the design most workshop material skips. The SP+Zerobus model vs. a "just grant MODIFY and INSERT" model differ in ways that matter once you leave the workshop:

| Dimension | Zerobus + SP (this lab) | Direct `INSERT` (attendees granted MODIFY) |
|---|---|---|
| **Producer identity in audit** | `workshop-zerobus-sp` — one row per ingest in the audit log, trivially traceable back to the Lab 4 flow. Queryable: `SELECT * FROM system.access.audit WHERE user_identity.email = 'workshop-zerobus-sp'`. | Each attendee's own user identity, mixed in with every other query they ran that session. Forensics have to filter by action type (`writeTable`) and table name. |
| **What the credential can do** | SP token is minted per-request with `authorization_details` pinning it to `USE CATALOG` + `USE SCHEMA` + `MODIFY/SELECT` on **this one table**. A leaked token can only append rows to `course_temp`. Cannot `DELETE`, cannot `DROP`, cannot touch any other table. | Attendee holds a PAT / session token with whatever grants they have. `MODIFY` on the table allows `INSERT`, `UPDATE`, `DELETE`, `MERGE`, `OPTIMIZE`, `VACUUM` — anything write-shaped. Harder to reason about. |
| **Revocation** | Rotate the SP's OAuth client secret in the setup notebook — old secret stops working, scope gets the new one, attendees notice nothing. No per-attendee churn. | Have to revoke/adjust grants on the group. If attendees wandered off with a PAT, the PAT keeps working until it's explicitly revoked. |
| **Credential surface** | Stored only in the `workshop` secret scope; `dbutils.secrets.get(...)` auto-redacts from notebook output; never lives in a notebook source file. | No extra credential — the attendee uses their own identity. Lower credential risk, but higher *authorization* risk because the identity is broad-purpose. |
| **Scales to 1,000 producers?** | Yes: each producer gets its own SP or they all share one; either way the path of least privilege stays the same. | Doesn't apply outside Databricks — external producers have no Databricks user identity to authenticate as. |

The TL;DR: the SP + scoped OAuth model **narrows the credential** (it can only do one thing on one table), **widens the audit trail** (you always know which producer flow wrote a row), and **scales to producers outside your workspace**. Direct `INSERT` is simpler for in-workspace use but can't express any of those properties.

### What to take away

- **Direct-to-Delta ingest** — one POST, one row, no intermediate bus. Ideal for edge devices or low-volume producers that live outside Databricks.
- **Fine-grained OAuth — a hotel keycard, not a master key.** `authorization_details` scopes the token to one table: a leaked token can append rows to `course_temp`, and nothing else. No `SELECT *`, no `DELETE`, no `DROP`.
- **Producer identity in audit** — `system.access.audit` attributes the write to `workshop-zerobus-sp`, not to the attendee. That's the right answer for "which pipeline produced this row?" queries.
- **Secret scope, not copy-paste** — attendees read creds from `dbutils.secrets.get(...)`, which the Databricks UI auto-redacts. Creds never land in notebook source, exports, or screen shares.
- **REST vs gRPC SDK trade-off** — REST is a handshake per record (higher per-record cost), gRPC holds a persistent stream (much higher throughput). For this workshop, one record per attendee, REST is the right call. At volume, use the SDK.
- **Zerobus does not create tables** — the table must exist with the exact schema before any record can land.

> Reference notebook: [`lab4-zerobus/send_temperature.py`](./lab4-zerobus/send_temperature.py).

---

## Lab 5 — Iceberg side-quest (optional)

> **Optional / take-home.** Skip if you're short on time — nothing else in the workshop depends on it. Run after Lab 1 or any time after; only the Bakehouse `sales_transactions` streaming table from Lab 1 is a prerequisite.

Publish a derived bakehouse result as a **managed Iceberg table** and read it back with **PyIceberg** through the Unity Catalog Iceberg REST endpoint — no Spark session required. That's the portability pitch of Iceberg on Unity Catalog: the same table that Databricks writes is readable by Trino, Snowflake, OSS Spark, or any pure-Python client.

### Step 5a — Top-5 sales locations as a managed Iceberg table (SQL, outside the pipeline)

The streaming table from Lab 1 is Delta. To make a derived result readable by any Iceberg-compatible engine without configuring an external location for UniForm Compatibility Mode, the simplest path is a **CTAS into a managed Iceberg table** — run it *outside* the pipeline in the SQL editor or a `%sql` cell.

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

### Step 5b — Read the Iceberg table with PyIceberg (Exploration notebook)

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

Expected output: a pandas DataFrame with the same 5 cities you saw in Step 5a, plus snapshot metadata for the Iceberg table.

**What you just demonstrated:**
- `pip install pyiceberg` — no cluster restart, no Iceberg JARs, no Spark session. A pure-Python client talks to Unity Catalog via the **Iceberg REST Catalog** endpoint at `/api/2.1/unity-catalog/iceberg-rest`.
- The `warehouse` parameter pins the UC catalog, so table identifiers collapse from three-part to two-part.
- External clients run this same code. Supply a PAT or OAuth token and the workspace URL — done. That's the portability of Iceberg on Unity Catalog.

**Requirements your admin has likely already set** (workshop attendees usually inherit these; flag with the instructor if any call fails with `403`):
- `EXTERNAL USE SCHEMA` on `workshop.<user>`
- External data access enabled on the workspace
- Workspace IP access list (if enabled) allows your client

> Reference copies of the CTAS and the PyIceberg reader live in [`lab5-iceberg/`](./lab5-iceberg/) alongside this guide.

---

## Wrap-up

You now have six tables across Labs 1-2-4, a full deployed bundle from Lab 3, and an optional Iceberg side-quest in Lab 5:

| Lab | Table | Type | Source | Language |
|---|---|---|---|---|
| 1 | `sales_transactions` | Streaming Table | `samples.bakehouse.sales_transactions` | Python |
| 1 | `sales_stats` | Materialized View (3 expectations) | `sales_transactions` | SQL |
| 2 | `bookings_current` | Streaming Table + AutoCDC | `samples.wanderbricks.booking_updates` | SQL |
| 2 | `booking_fraud_flags` | Streaming Table + Auto Loader | `/Volumes/workshop/shared/landing/booking_fraud_flags/` | SQL |
| 2 | `payments` | Streaming Table | `samples.wanderbricks.payments` | SQL |
| 2 | `booking_fraud_summary` | Materialized View | `bookings_current` ⨝ `payments` ⨝ `booking_fraud_flags` | SQL |
| 3 | *(bundle)* | SDP pipelines + workflow + AI/BI dashboard deployed from `databricks/tmm/Lakeflow-Gourmet-Pipeline` | `workshop.<user>` | SQL |
| 4 | `workshop.zerobus.course_temp` | Managed Delta table (shared), written via Zerobus REST | HTTP POST from attendee notebook | Python |
| 5 *(optional)* | `global_sales_gold` | Managed Iceberg table (CTAS, outside pipeline) | `sales_transactions` ⨝ `samples.bakehouse.sales_franchises` | SQL |

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
| **Toggle Agent mode** | **Agent** / **Chat** mode selector at the bottom of the Genie Code pane |
| Clone a Git folder | Workspace sidebar **Create → Git folder** (sparse checkout supported) |
| Deploy a bundle from UI | Open the bundle folder → **Deployments** icon (🚀) in the left pane |
