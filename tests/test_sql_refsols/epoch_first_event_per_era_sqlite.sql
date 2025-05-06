WITH _t AS (
  SELECT
    eras.er_name AS name,
    events.ev_name AS name_1,
    eras.er_start_year AS start_year,
    ROW_NUMBER() OVER (PARTITION BY eras.er_name ORDER BY events.ev_dt) AS _w
  FROM eras AS eras
  JOIN events AS events
    ON eras.er_end_year > CAST(STRFTIME('%Y', events.ev_dt) AS INTEGER)
    AND eras.er_start_year <= CAST(STRFTIME('%Y', events.ev_dt) AS INTEGER)
)
SELECT
  name AS era_name,
  name_1 AS event_name
FROM _t
WHERE
  _w = 1
ORDER BY
  start_year
