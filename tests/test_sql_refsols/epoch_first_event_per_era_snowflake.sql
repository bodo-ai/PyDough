WITH _t1 AS (
  SELECT
    eras.er_name,
    eras.er_start_year,
    events.ev_name
  FROM eras AS eras
  JOIN events AS events
    ON eras.er_end_year > YEAR(CAST(events.ev_dt AS TIMESTAMP))
    AND eras.er_start_year <= YEAR(CAST(events.ev_dt AS TIMESTAMP))
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY eras.er_name ORDER BY events.ev_dt) = 1
)
SELECT
  er_name AS era_name,
  ev_name AS event_name
FROM _t1
ORDER BY
  er_start_year NULLS FIRST
