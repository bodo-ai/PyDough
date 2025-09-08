WITH _t2 AS (
  SELECT
    eras.er_end_year,
    eras.er_name,
    eras.er_start_year,
    events.ev_dt,
    DATEDIFF(
      DAY,
      CAST(LAG(events.ev_dt, 1) OVER (PARTITION BY eras.er_name, eras.er_name ORDER BY events.ev_dt) AS DATETIME),
      CAST(events.ev_dt AS DATETIME)
    ) AS day_gap
  FROM eras AS eras
  JOIN events AS events
    ON eras.er_end_year > YEAR(CAST(events.ev_dt AS TIMESTAMP))
    AND eras.er_start_year <= YEAR(CAST(events.ev_dt AS TIMESTAMP))
)
SELECT
  er_name AS era_name,
  AVG(day_gap) AS avg_event_gap
FROM _t2
WHERE
  er_end_year > YEAR(CAST(ev_dt AS TIMESTAMP))
  AND er_start_year <= YEAR(CAST(ev_dt AS TIMESTAMP))
GROUP BY
  1
ORDER BY
  ANY_VALUE(er_start_year) NULLS FIRST
