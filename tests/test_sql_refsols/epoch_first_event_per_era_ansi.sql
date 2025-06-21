WITH _t1 AS (
  SELECT
    eras.er_name AS name,
    events.ev_name AS name_1,
    eras.er_start_year AS start_year
  FROM eras AS eras
  JOIN events AS events
    ON eras.er_end_year > EXTRACT(YEAR FROM CAST(events.ev_dt AS DATETIME))
    AND eras.er_start_year <= EXTRACT(YEAR FROM CAST(events.ev_dt AS DATETIME))
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY eras.er_name ORDER BY events.ev_dt NULLS LAST) = 1
)
SELECT
  name AS era_name,
  name_1 AS event_name
FROM _t1
ORDER BY
  start_year
