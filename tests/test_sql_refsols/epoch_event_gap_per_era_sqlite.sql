WITH _t1 AS (
  SELECT
    CAST((
      JULIANDAY(DATE(_s1.ev_dt, 'start of day')) - JULIANDAY(
        DATE(
          LAG(_s1.ev_dt, 1) OVER (PARTITION BY _s0.er_name ORDER BY _s1.ev_dt),
          'start of day'
        )
      )
    ) AS INTEGER) AS day_gap,
    _s0.er_name AS name,
    _s0.er_start_year AS start_year
  FROM eras AS _s0
  JOIN events AS _s1
    ON _s0.er_end_year > CAST(STRFTIME('%Y', _s1.ev_dt) AS INTEGER)
    AND _s0.er_start_year <= CAST(STRFTIME('%Y', _s1.ev_dt) AS INTEGER)
), _t0 AS (
  SELECT
    MAX(start_year) AS agg_3,
    AVG(day_gap) AS avg_event_gap,
    MAX(name) AS era_name
  FROM _t1
  GROUP BY
    name
)
SELECT
  era_name,
  avg_event_gap
FROM _t0
ORDER BY
  agg_3
