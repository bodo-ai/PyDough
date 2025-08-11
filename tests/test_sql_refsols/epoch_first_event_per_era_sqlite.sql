WITH _t AS (
  SELECT
    eras.er_end_year,
    eras.er_name,
    eras.er_start_year,
    events.ev_dt,
    events.ev_name,
    ROW_NUMBER() OVER (PARTITION BY eras.er_name ORDER BY events.ev_dt) AS _w
  FROM eras AS eras
  JOIN events AS events
    ON eras.er_end_year > CAST(STRFTIME('%Y', events.ev_dt) AS INTEGER)
    AND eras.er_start_year <= CAST(STRFTIME('%Y', events.ev_dt) AS INTEGER)
)
SELECT
  er_name AS era_name,
  ev_name AS event_name
FROM _t
WHERE
  _w = 1
  AND er_end_year > CAST(STRFTIME('%Y', ev_dt) AS INTEGER)
  AND er_start_year <= CAST(STRFTIME('%Y', ev_dt) AS INTEGER)
ORDER BY
  er_start_year
