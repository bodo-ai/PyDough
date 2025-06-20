WITH _t1 AS (
  SELECT
    DATEDIFF(
      _s1.ev_dt,
      LAG(_s1.ev_dt, 1) OVER (PARTITION BY _s0.er_name ORDER BY _s1.ev_dt NULLS LAST),
      DAY
    ) AS day_gap,
    _s0.er_name AS name,
    _s0.er_start_year AS start_year
  FROM eras AS _s0
  JOIN events AS _s1
    ON _s0.er_end_year > EXTRACT(YEAR FROM _s1.ev_dt)
    AND _s0.er_start_year <= EXTRACT(YEAR FROM _s1.ev_dt)
), _t0 AS (
  SELECT
    ANY_VALUE(start_year) AS agg_3,
    AVG(day_gap) AS avg_event_gap,
    ANY_VALUE(name) AS era_name
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
