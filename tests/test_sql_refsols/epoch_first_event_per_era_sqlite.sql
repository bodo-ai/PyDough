WITH _t AS (
  SELECT
    _s0.er_name AS name,
    _s1.ev_name AS name_1,
    _s0.er_start_year AS start_year,
    ROW_NUMBER() OVER (PARTITION BY _s0.er_name ORDER BY _s1.ev_dt) AS _w
  FROM eras AS _s0
  JOIN events AS _s1
    ON _s0.er_end_year > CAST(STRFTIME('%Y', _s1.ev_dt) AS INTEGER)
    AND _s0.er_start_year <= CAST(STRFTIME('%Y', _s1.ev_dt) AS INTEGER)
)
SELECT
  name AS era_name,
  name_1 AS event_name
FROM _t
WHERE
  _w = 1
ORDER BY
  start_year
