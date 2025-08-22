WITH _t1 AS (
  SELECT
    EXTRACT(EPOCH FROM CAST(events.ev_dt AS TIMESTAMP) - CAST(LAG(events.ev_dt, 1) OVER (PARTITION BY eras.er_name ORDER BY events.ev_dt) AS TIMESTAMP)) / 86400 AS day_gap,
    eras.er_name,
    eras.er_start_year
  FROM eras AS eras
  JOIN events AS events
    ON eras.er_end_year > EXTRACT(YEAR FROM CAST(events.ev_dt AS TIMESTAMP))
    AND eras.er_start_year <= EXTRACT(YEAR FROM CAST(events.ev_dt AS TIMESTAMP))
)
SELECT
  er_name AS era_name,
  AVG(day_gap) AS avg_event_gap
FROM _t1
GROUP BY
  er_name
ORDER BY
  MAX(er_start_year) NULLS FIRST
