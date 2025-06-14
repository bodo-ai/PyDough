WITH _t1 AS (
  SELECT
    DATEDIFF(
      events.ev_dt,
      LAG(events.ev_dt, 1) OVER (PARTITION BY eras.er_name ORDER BY events.ev_dt NULLS LAST),
      DAY
    ) AS day_gap,
    eras.er_name AS name,
    eras.er_start_year AS start_year
  FROM eras AS eras
  JOIN events AS events
    ON eras.er_end_year > EXTRACT(YEAR FROM events.ev_dt)
    AND eras.er_start_year <= EXTRACT(YEAR FROM events.ev_dt)
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
