-- Sample analyst queries against the curated model (and raw where useful).
-- Each filters on the partition column (event_date) so BigQuery prunes
-- partitions and scans only what it needs.

-- 1) Daily totals and depth per day in May 2018.
SELECT
  event_date,
  SUM(event_count)                         AS total_events,
  ROUND(SUM(avg_depth_km * event_count)
        / SUM(event_count), 2)             AS depth_weighted_avg_km
FROM `${PROJECT}.model.daily_mag_buckets`
WHERE event_date BETWEEN '2018-05-01' AND '2018-05-31'
GROUP BY event_date
ORDER BY event_date;


-- 2) Magnitude-bucket mix over the whole window: how many days had a >=4.5 event,
--    and what share of events fell in each bucket.
SELECT
  mag_bucket,
  SUM(event_count)                                          AS events,
  COUNT(DISTINCT event_date)                                AS days_present,
  ROUND(100 * SUM(event_count)
        / SUM(SUM(event_count)) OVER (), 1)                 AS pct_of_all_events
FROM `${PROJECT}.model.daily_mag_buckets`
WHERE event_date BETWEEN '2018-05-01' AND '2018-05-31'
GROUP BY mag_bucket
ORDER BY events DESC;


-- 3) 7-day rolling event count (trend), from the daily rollup.
WITH daily AS (
  SELECT event_date, SUM(event_count) AS events
  FROM `${PROJECT}.model.daily_mag_buckets`
  WHERE event_date BETWEEN '2018-05-01' AND '2018-05-31'
  GROUP BY event_date
)
SELECT
  event_date,
  events,
  SUM(events) OVER (
    ORDER BY event_date
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ) AS rolling_7d_events
FROM daily
ORDER BY event_date;
