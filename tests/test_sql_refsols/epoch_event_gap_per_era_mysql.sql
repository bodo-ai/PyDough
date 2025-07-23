WITH _t1 AS (
  SELECT
    DATEDIFF(
      CAST(EVENTS.ev_dt AS DATETIME),
      CAST(LAG(EVENTS.ev_dt, 1) OVER (PARTITION BY ERAS.er_name ORDER BY CASE WHEN EVENTS.ev_dt IS NULL THEN 1 ELSE 0 END, EVENTS.ev_dt) AS DATETIME)
    ) AS day_gap,
    ERAS.er_name,
    ERAS.er_start_year
  FROM ERAS AS ERAS
  JOIN EVENTS AS EVENTS
    ON ERAS.er_end_year > YEAR(EVENTS.ev_dt)
    AND ERAS.er_start_year <= YEAR(EVENTS.ev_dt)
)
SELECT
  er_name AS era_name,
  AVG(day_gap) AS avg_event_gap
FROM _t1
GROUP BY
  er_name
ORDER BY
  ANY_VALUE(er_start_year)
