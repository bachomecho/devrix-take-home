# AI Usage

AI assistance (Claude) was used on this project, but it was deliberately kept to
a narrow scope. The pipeline design, the USGS API handling, and the overall
structure of `etl.py` were written and reasoned through by hand.

## Where AI was used

**1. Documentation**

Most of the AI help went into writing prose: this file, the `README.md`
(quickstart, parameter table, backfill instructions, trade-offs section), and the
notes accompanying the architecture diagram. The content and the decisions being
described were mine — AI mainly helped phrase them clearly and keep the structure
consistent.

**2. SQL queries**

Used as a sounding board for the BigQuery SQL in `sql/`:

- Getting the `MERGE` in `02_merge_raw.sql` right — in particular the
  `WHEN MATCHED AND s.updated > t.updated` guard that makes re-runs a no-op.
- The `DELETE ... WHERE event_date BETWEEN` + `INSERT` window pattern in
  `03_build_model.sql` for idempotent partition rebuilds.
- Syntax and readability of the analyst queries in `04_sample_queries.sql`
  (window functions for the 7-day rolling count, the bucket-share percentages).
- Confirming partitioning/clustering syntax in `01_ddl_raw.sql`.

**3. Airflow orchestration**

The bonus DAG in `dags/usgs_etl_dag.py` — task structure, how to import and reuse
`etl.py`'s functions from the DAG, and the local `airflow standalone` /
`airflow dags test` workflow. I have less day-to-day Airflow experience than
Python, so this was the area where AI sped things up the most.
