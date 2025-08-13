WITH _t2 AS (
  SELECT
    DATEDIFF(
      CAST(events.ev_dt AS DATETIME),
      CAST(LAG(events.ev_dt, 1) OVER (PARTITION BY eras.er_name, eras.er_name ORDER BY events.ev_dt NULLS LAST) AS DATETIME),
      DAY
    ) AS day_gap,
    eras.er_end_year,
    eras.er_name,
    eras.er_start_year,
    events.ev_dt
  FROM eras AS eras
  JOIN events AS events
    ON eras.er_end_year > EXTRACT(YEAR FROM CAST(events.ev_dt AS DATETIME))
    AND eras.er_start_year <= EXTRACT(YEAR FROM CAST(events.ev_dt AS DATETIME))
)
SELECT
  er_name AS era_name,
  AVG(day_gap) AS avg_event_gap
FROM _t2
WHERE
  er_end_year > EXTRACT(YEAR FROM CAST(ev_dt AS DATETIME))
  AND er_start_year <= EXTRACT(YEAR FROM CAST(ev_dt AS DATETIME))
GROUP BY
  er_name
ORDER BY
  ANY_VALUE(er_start_year)
