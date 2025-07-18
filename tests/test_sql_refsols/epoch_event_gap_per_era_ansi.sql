WITH _t1 AS (
  SELECT
    DATEDIFF(
      CAST(events.ev_dt AS DATETIME),
      CAST(LAG(events.ev_dt, 1) OVER (PARTITION BY eras.er_name ORDER BY events.ev_dt NULLS LAST) AS DATETIME),
      DAY
    ) AS day_gap,
    eras.er_name,
    eras.er_start_year
  FROM eras AS eras
  JOIN events AS events
    ON eras.er_end_year > EXTRACT(YEAR FROM CAST(events.ev_dt AS DATETIME))
    AND eras.er_start_year <= EXTRACT(YEAR FROM CAST(events.ev_dt AS DATETIME))
)
SELECT
  ANY_VALUE(er_name) AS era_name,
  AVG(day_gap) AS avg_event_gap
FROM _t1
GROUP BY
  er_name
ORDER BY
  ANY_VALUE(er_start_year)
