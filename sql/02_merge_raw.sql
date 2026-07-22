MERGE `${PROJECT}.raw.events` AS t
USING `${PROJECT}.raw._stage_events` AS s
ON t.id = s.id
WHEN MATCHED AND s.updated > t.updated THEN UPDATE SET
  event_time = s.event_time,
  updated    = s.updated,
  mag        = s.mag,
  depth_km   = s.depth_km,
  latitude   = s.latitude,
  longitude  = s.longitude,
  place      = s.place,
  type       = s.type,
  status     = s.status
WHEN NOT MATCHED THEN
  INSERT ROW; -- idempotency
