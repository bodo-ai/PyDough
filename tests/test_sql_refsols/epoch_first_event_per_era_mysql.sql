WITH _t AS (
  SELECT
    eras.er_name,
    eras.er_start_year,
    events.ev_name,
    ROW_NUMBER() OVER (PARTITION BY eras.er_name ORDER BY CASE WHEN events.ev_dt IS NULL THEN 1 ELSE 0 END, events.ev_dt) AS _w
  FROM eras AS eras
  JOIN events AS events
    ON eras.er_end_year > EXTRACT(YEAR FROM CAST(events.ev_dt AS DATETIME))
    AND eras.er_start_year <= EXTRACT(YEAR FROM CAST(events.ev_dt AS DATETIME))
)
SELECT
  er_name AS era_name,
  ev_name AS event_name
FROM _t
WHERE
  _w = 1
ORDER BY
  er_start_year
