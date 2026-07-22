#!/usr/bin/env python3
"""
USGS Earthquakes -> BigQuery batch ETL (minimal single-file CLI).

Flow:  USGS API (GeoJSON) -> flatten -> stage -> MERGE into raw.events
       -> rebuild model.daily_mag_buckets

Auth:  gcloud auth application-default login   (or set GOOGLE_APPLICATION_CREDENTIALS)
       export GOOGLE_CLOUD_PROJECT=your-project

Run:   python etl.py --project $GOOGLE_CLOUD_PROJECT \
           --start-date 2018-05-01 --end-date 2018-05-31
"""
import argparse
import datetime as dt
import os, sys, logging, requests
from google.cloud import bigquery

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("usgs_etl")

API = "https://earthquake.usgs.gov/fdsnws/event/1/query"

RAW_SCHEMA = [
    bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("event_time", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("updated", "TIMESTAMP"),
    bigquery.SchemaField("mag", "FLOAT64"),
    bigquery.SchemaField("depth_km", "FLOAT64"),
    bigquery.SchemaField("latitude", "FLOAT64"),
    bigquery.SchemaField("longitude", "FLOAT64"),
    bigquery.SchemaField("place", "STRING"),
    bigquery.SchemaField("type", "STRING"),
    bigquery.SchemaField("status", "STRING"),
]


def fetch(args):
    """Page through the USGS API and yield flattened rows."""
    offset = 1
    while True:
        params = {
            "format": "geojson",
            "starttime": args.start_date,
            "endtime": (dt.date.fromisoformat(args.end_date)
                        + dt.timedelta(days=1)).isoformat(),  # inclusive end
            "minmagnitude": args.min_magnitude,
            "minlatitude": args.min_latitude, "maxlatitude": args.max_latitude,
            "minlongitude": args.min_longitude, "maxlongitude": args.max_longitude,
            "orderby": "time-asc", "limit": args.page_limit, "offset": offset,
        }
        features = requests.get(API, params=params, timeout=120).json()["features"]
        if not features:
            break
        for f in features:
            props, coords = f["properties"], f["geometry"]["coordinates"]
            yield {
                "id": f["id"],
                "event_time": ms_to_iso(props["time"]),
                "updated": ms_to_iso(props.get("updated")),
                "mag": props.get("mag"),
                "depth_km": coords[2] if len(coords) > 2 else None,
                "latitude": coords[1], "longitude": coords[0],
                "place": props.get("place"),
                "type": props.get("type"),
                "status": props.get("status"),
            }
        if len(features) < args.page_limit:
            break
        offset += args.page_limit


def ms_to_iso(ms):
    """USGS times are epoch milliseconds (UTC)."""
    if ms is None:
        return None
    return dt.datetime.fromtimestamp(ms / 1000, tz=dt.timezone.utc).isoformat()


def ensure_dataset(client, ds_id):
    # Create the dataset, and clear any default partition expiration.
    ds = client.create_dataset(bigquery.Dataset(ds_id), exists_ok=True)
    if ds.default_partition_expiration_ms:
        log.warning("Dataset %s has default_partition_expiration_ms=%s "
                    "(sandbox leftover) -> clearing it.",
                    ds_id, ds.default_partition_expiration_ms)
        ds.default_partition_expiration_ms = None
        client.update_dataset(ds, ["default_partition_expiration_ms"])


def load_raw(client, args, rows):
    #Stage the batch, then MERGE-upsert into the partitioned raw table.
    raw = f"{args.project}.raw.events"
    stage = f"{args.project}.raw._stage_events"
    log.info("Target project=%s  raw=%s", args.project, raw)

    ensure_dataset(client, f"{args.project}.raw")

    # Create raw.events partitioned by event date if it doesn't exist.
    table = bigquery.Table(raw, schema=RAW_SCHEMA)
    table.time_partitioning = bigquery.TimePartitioning(field="event_time")
    tbl = client.create_table(table, exists_ok=True)
    # Clear a partition expiration inherited from sandbox mode (see above).
    if tbl.time_partitioning and tbl.time_partitioning.expiration_ms:
        log.warning("Table %s has partition expiration=%s ms -> clearing it.",
                    raw, tbl.time_partitioning.expiration_ms)
        tbl.time_partitioning.expiration_ms = None
        client.update_table(tbl, ["time_partitioning"])

    # Load batch into a truncate-on-write staging table.
    load_job = client.load_table_from_json(
        rows, stage,
        job_config=bigquery.LoadJobConfig(
            schema=RAW_SCHEMA,
            write_disposition="WRITE_TRUNCATE",
        ),
    )
    load_job.result()
    staged = client.get_table(stage).num_rows
    log.info("Staged %d rows into %s (load job %s)", staged, stage, load_job.job_id)

    # Idempotent upsert on the stable key `id`; only newer inserts win.
    merge_job = client.query(f"""
        MERGE `{raw}` t
        USING `{stage}` s ON t.id = s.id
        WHEN MATCHED AND s.updated > t.updated THEN UPDATE SET
          event_time=s.event_time, updated=s.updated, mag=s.mag,
          depth_km=s.depth_km, latitude=s.latitude, longitude=s.longitude,
          place=s.place, type=s.type, status=s.status
        WHEN NOT MATCHED THEN INSERT ROW
    """)
    merge_job.result()
    log.info("MERGE affected %s row(s) in %s (job %s)",
             merge_job.num_dml_affected_rows, raw, merge_job.job_id)
    total = list(client.query(f"SELECT COUNT(*) c FROM `{raw}`").result())[0].c
    log.info("raw.events now holds %d row(s) total.", total)


def build_model(client, args):
    # creating target table
    model = f"{args.project}.model.daily_mag_buckets"
    ensure_dataset(client, f"{args.project}.model")
    client.query(f"""
        CREATE TABLE IF NOT EXISTS `{model}` (
          event_date DATE, mag_bucket STRING,
          event_count INT64, avg_depth_km FLOAT64
        ) PARTITION BY event_date;

        DELETE FROM `{model}`
        WHERE event_date BETWEEN '{args.start_date}' AND '{args.end_date}';

        INSERT INTO `{model}`
        SELECT DATE(event_time),
               CASE WHEN mag < 3.5 THEN '<3.5'
                    WHEN mag < 4.5 THEN '3.5-4.4'
                    ELSE '>=4.5' END,
               COUNT(*), ROUND(AVG(depth_km), 2)
        FROM `{args.project}.raw.events`
        WHERE DATE(event_time) BETWEEN '{args.start_date}' AND '{args.end_date}'
          AND mag IS NOT NULL
        GROUP BY 1, 2
    """).result() # groups by date, mag
    built = list(client.query(f"SELECT COUNT(*) c FROM `{model}`").result())[0].c
    log.info("model.daily_mag_buckets now holds %d row(s) total.", built)


def main():
    p = argparse.ArgumentParser(description="USGS earthquakes -> BigQuery ETL.")
    p.add_argument("--project", default=os.environ.get("GOOGLE_CLOUD_PROJECT"),
                   required=os.environ.get("GOOGLE_CLOUD_PROJECT") is None)
    p.add_argument("--start-date", default="2018-05-01")
    p.add_argument("--end-date", default="2018-05-31")
    p.add_argument("--min-magnitude", type=float, default=3.0)
    p.add_argument("--min-latitude", type=float, default=32.0)
    p.add_argument("--max-latitude", type=float, default=42.0)
    p.add_argument("--min-longitude", type=float, default=-125.0)
    p.add_argument("--max-longitude", type=float, default=-114.0)
    p.add_argument("--page-limit", type=int, default=20000)
    p.add_argument("--reset", action="store_true",
                   help="Drop the raw and model datasets before running. Use once "
                        "to clear tables created under sandbox partition expiration.")
    args = p.parse_args()
    log.info("Run scope: project=%s  %s..%s  minmag=%s  bbox[lat %s..%s, lon %s..%s]",
             args.project, args.start_date, args.end_date, args.min_magnitude,
             args.min_latitude, args.max_latitude,
             args.min_longitude, args.max_longitude)

    try:
        rows = list(fetch(args))
    except Exception:
        log.exception("Fetch from USGS failed (network/proxy or bad params).")
        sys.exit(1)
    log.info("Fetched %d events from USGS.", len(rows))
    if not rows:
        log.warning("No events returned for this scope; nothing to load. "
                    "Check the date range / bbox / magnitude filters.")
        return

    try:
        client = bigquery.Client(project=args.project)
        if args.reset:
            for ds in ("raw", "model"):
                client.delete_dataset(f"{args.project}.{ds}",
                                      delete_contents=True, not_found_ok=True)
                log.warning("--reset: dropped dataset %s.%s", args.project, ds)
        load_raw(client, args, rows)
        build_model(client, args)
    except Exception as e:
        log.exception("BigQuery step failed: %s", e)
        # Ran into billing problems, so that's logging for that
        if "billingNotEnabled" in str(e):
            log.error("This project is still in a sandbox and billing is not enabled.", args.project)
        sys.exit(1)

    log.info("DONE: loaded raw.events and rebuilt model.daily_mag_buckets "
             "for %s..%s.", args.start_date, args.end_date)


if __name__ == "__main__":
    main()
