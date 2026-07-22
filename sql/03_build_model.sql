-- Curated model: daily earthquake counts by magnitude bucket.
-- Partitioned by event_date. The DELETE+INSERT is scoped to the run's date

CREATE SCHEMA IF NOT EXISTS `${PROJECT}.model`
OPTIONS (location = 'US');

CREATE TABLE IF NOT EXISTS `${PROJECT}.model.daily_mag_buckets`
(
  event_date   DATE,
  mag_bucket   STRING  OPTIONS (description = '<3.5 | 3.5-4.4 | >=4.5'),
  event_count  INT64,
  avg_depth_km FLOAT64
)
PARTITION BY event_date
OPTIONS (description = 'Daily quake counts by magnitude bucket (curated).');

-- Reprocess only the target window.
DELETE FROM `${PROJECT}.model.daily_mag_buckets`
WHERE event_date BETWEEN DATE('${START_DATE}') AND DATE('${END_DATE}');

INSERT INTO `${PROJECT}.model.daily_mag_buckets`
SELECT
  DATE(event_time) AS event_date,
  CASE
    WHEN mag <  3.5 THEN '<3.5'
    WHEN mag <  4.5 THEN '3.5-4.4'
    ELSE '>=4.5'
  END AS mag_bucket,
  COUNT(*)                AS event_count,
  ROUND(AVG(depth_km), 2) AS avg_depth_km
FROM `${PROJECT}.raw.events`
WHERE DATE(event_time) BETWEEN DATE('${START_DATE}') AND DATE('${END_DATE}')
  AND mag IS NOT NULL
GROUP BY event_date, mag_bucket;
