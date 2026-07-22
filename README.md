# USGS Earthquakes → BigQuery ETL

A small, idempotent batch ETL that pulls earthquake events from the
[USGS FDSN Event API](https://earthquake.usgs.gov/fdsnws/event/1/) into BigQuery,
then builds one curated model table. It demonstrates incremental/idempotent
loads, backfills/reprocessing, and partitioning — while staying inside the
BigQuery free tier.

- **Raw layer:** `raw.events` — one flattened row per event, partitioned by event
  date, clustered by `mag, place`.
- **Model layer:** `model.daily_mag_buckets` — daily quake counts by magnitude
  bucket (`<3.5`, `3.5–4.4`, `≥4.5`) with `event_count` and `avg_depth_km`.

---

## Quickstart (local)

```bash
# 1. Install deps (a venv is recommended)
pip install -r requirements.txt

# 2. Authenticate with Application Default Credentials (no secrets in the repo)
gcloud auth application-default login
# OR
export GOOGLE_APPLICATION_CREDENTIALS = "path-to-key-json"
export GOOGLE_CLOUD_PROJECT=devrix-take-gome

# 3. Run the full pipeline for the fixed scope (California, May 2018, M3.0+)
python etl.py --project $GOOGLE_CLOUD_PROJECT \
    --start-date 2018-05-01 --end-date 2018-05-31
```

The script creates the `raw` and `model` datasets/tables if missing, so no manual
setup is required. The `sql/*.sql` files document the same DDL/DML for reference
and can be run directly with `bq query` if you prefer.

---

## Parameters

Every value is configurable via CLI flag. Defaults match the fixed challenge
scope (California bounding box, May 2018, M3.0+).

| Flag | Default | Meaning |
|------|---------|---------|
| `--project` | `$GOOGLE_CLOUD_PROJECT` | GCP project id (required if env var unset) |
| `--start-date` | `2018-05-01` | Inclusive start (YYYY-MM-DD) |
| `--end-date` | `2018-05-31` | Inclusive end (YYYY-MM-DD) |
| `--min-magnitude` | `3.0` | `minmagnitude` |
| `--min-latitude` / `--max-latitude` | `32` / `42` | Bounding box |
| `--min-longitude` / `--max-longitude` | `-125` / `-114` | Bounding box |
| `--page-limit` | `20000` | Page size (`limit`, ≤ 20000) |

**Paging:** the API caps `limit` at 20000 and uses a 1-based `offset`. `etl.py`
pages with `orderby=time-asc` until a short/empty page is returned. `endtime` is
advanced by one day internally so the end date is inclusive.

---

## Incremental & idempotent approach

**Stable key:** the USGS event `id`.

**Upsert:** each batch lands in `raw._stage_events` (truncate-on-write), then a
`MERGE` upserts into `raw.events`:

- `WHEN MATCHED AND s.updated > t.updated` → update (only genuine revisions win).
- `WHEN NOT MATCHED` → insert.

Because the match key is stable and updates are gated on a newer `updated`
timestamp, **re-running the same window is a no-op** and **backfills never create
duplicates**.

---

## Backfill / reprocessing instructions

Reprocess any window by passing the date range — the model rebuild is scoped with
`DELETE ... WHERE event_date BETWEEN start AND end` followed by `INSERT`, so only
those partitions are touched and the result is idempotent:

```bash
# Re-run a single day
python etl.py --project $PROJECT --start-date 2018-05-14 --end-date 2018-05-14

# Re-run the whole month
python etl.py --project $PROJECT --start-date 2018-05-01 --end-date 2018-05-31

# Different region / threshold (all configurable)
python etl.py --project $PROJECT \
    --start-date 2020-01-01 --end-date 2020-01-31 \
    --min-latitude 18 --max-latitude 23 --min-longitude -160 --max-longitude -154 \
    --min-magnitude 2.5
```

---

## Partitioning & clustering

- `raw.events` — `PARTITION BY DATE(event_time)`, `CLUSTER BY mag, place`.
- `model.daily_mag_buckets` — `PARTITION BY event_date`.

Every analyst query and every ETL write filters on the partition column, so
BigQuery prunes to the relevant days and scan cost stays proportional to the
window — not the whole table. The May-2018 California M3.0+ window is only a few
hundred events, so storage and scan sit comfortably in the free tier.

---

## Schema evolution

Schema evolution is a table's structure changing over time while it stays in use
— columns appearing, types widening, fields dropped or renamed — and the pipeline
absorbing that without breaking or losing data. It matters because upstream
sources change on their own schedule: USGS could add a `felt` or `tsunami`
property to their GeoJSON tomorrow, and this table was created without it.

Changes, roughly safest to riskiest:

- **Add a column** — safest; existing rows read `NULL`. BigQuery supports it
  directly (`ALLOW_FIELD_ADDITION` on a load job).
- **Widen a type** (`INT64`→`FLOAT64`, `REQUIRED`→`NULLABLE`) — safe; every
  existing value is still valid under the new type.
- **Narrow a type** (`FLOAT64`→`INT64`, `NULLABLE`→`REQUIRED`) — unsafe; existing
  rows may violate the new constraint, so it usually means rewriting the table.
- **Drop or rename** — breaks any reader referencing the column; a rename is
  really a drop plus an add unless column identity is tracked separately.

The useful mental frame is two directions of compatibility: **backward
compatible** means new code can read old data; **forward compatible** means old
code can read new data.

**How this pipeline handles it:** it loads with an explicit `RAW_SCHEMA` and
upserts with `MERGE` on the stable `id`, so additive and widening changes are
absorbed safely. Capturing a new USGS property (e.g. `felt`) is a one-line change
— add a `NULLABLE` field to `RAW_SCHEMA`; older partitions simply keep `NULL` for
it, with no rewrite and no failure. Narrowing and renaming are deliberately *not*
automated, since those are the unsafe cases that warrant a manual migration.

---
## Sample analyst queries

See `sql/04_sample_queries.sql`:

1. Daily totals + depth-weighted average depth.
2. Magnitude-bucket mix and each bucket's share of all events.
3. 7-day rolling event count (trend).

---

## GCP access for review

- Uses **your own GCP project** and **Application Default Credentials** (no keys
  in the repo).
- Grant the reviewer **Project Owner** (revoke after review).
- **Project ID:** `devrix-take-home`
- **Datasets:** `raw`, `model`

---

## Orchestration options

- **A. CLI (required):** `etl.py` — one file, end-to-end, parameterized. ✅
- **B. Airflow DAG (bonus):** `dags/usgs_etl_dag.py` reuses `etl.py`'s functions;
  runnable locally via `airflow standalone` / `airflow dags test`.

---
