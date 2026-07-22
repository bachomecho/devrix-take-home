-- Raw layer: one row per USGS event, upserted on the stable key `id`.
-- Partitioned by event date so the ETL and analysts prune on DATE(event_time).
-- Clustered by mag/place to speed common magnitude/region filters.
--
-- Replace ${PROJECT} before running, or run via `bq query` with substitution.

CREATE SCHEMA IF NOT EXISTS `${PROJECT}.raw`
OPTIONS (location = 'US');

CREATE TABLE IF NOT EXISTS `${PROJECT}.raw.events`
(
  id          STRING    NOT NULL OPTIONS (description = 'USGS event id (stable key)'),
  event_time  TIMESTAMP NOT NULL OPTIONS (description = 'Event origin time (UTC)'),
  updated     TIMESTAMP          OPTIONS (description = 'Last time USGS updated the event'),
  mag         FLOAT64            OPTIONS (description = 'Magnitude'),
  depth_km    FLOAT64            OPTIONS (description = 'Depth in km (geometry.coordinates[2])'),
  latitude    FLOAT64,
  longitude   FLOAT64,
  place       STRING,
  type        STRING             OPTIONS (description = 'e.g. earthquake, quarry blast'),
  status      STRING             OPTIONS (description = 'automatic | reviewed')
)
PARTITION BY DATE(event_time)
CLUSTER BY mag, place
OPTIONS (description = 'Flattened USGS earthquake events (raw layer).');
