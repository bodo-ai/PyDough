WITH _t2 AS (
  SELECT
    DATEDIFF(
      EVENTS.ev_dt,
      LAG(EVENTS.ev_dt, 1) OVER (PARTITION BY ERAS.er_name, ERAS.er_name ORDER BY CASE WHEN EVENTS.ev_dt IS NULL THEN 1 ELSE 0 END, EVENTS.ev_dt)
    ) AS day_gap,
    ERAS.er_end_year,
    ERAS.er_name,
    ERAS.er_start_year,
    EVENTS.ev_dt
  FROM ERAS AS ERAS
  JOIN EVENTS AS EVENTS
    ON ERAS.er_end_year > EXTRACT(YEAR FROM CAST(EVENTS.ev_dt AS DATETIME))
    AND ERAS.er_start_year <= EXTRACT(YEAR FROM CAST(EVENTS.ev_dt AS DATETIME))
)
SELECT
  er_name AS era_name,
  AVG(day_gap) AS avg_event_gap
FROM _t2
WHERE
  er_end_year > EXTRACT(YEAR FROM CAST(ev_dt AS DATETIME))
  AND er_start_year <= EXTRACT(YEAR FROM CAST(ev_dt AS DATETIME))
GROUP BY
  1
ORDER BY
  ANY_VALUE(er_start_year)
