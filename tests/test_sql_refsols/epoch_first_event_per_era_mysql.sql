WITH _t AS (
  SELECT
    ERAS.er_name,
    ERAS.er_start_year,
    EVENTS.ev_name,
    ROW_NUMBER() OVER (PARTITION BY ERAS.er_name ORDER BY CASE WHEN EVENTS.ev_dt IS NULL THEN 1 ELSE 0 END, EVENTS.ev_dt) AS _w
  FROM ERAS AS ERAS
  JOIN EVENTS AS EVENTS
    ON ERAS.er_end_year > EXTRACT(YEAR FROM CAST(EVENTS.ev_dt AS DATETIME))
    AND ERAS.er_start_year <= EXTRACT(YEAR FROM CAST(EVENTS.ev_dt AS DATETIME))
)
SELECT
  er_name AS era_name,
  ev_name AS event_name
FROM _t
WHERE
  _w = 1
ORDER BY
  er_start_year
