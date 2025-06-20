WITH _T1 AS (
  SELECT
    ERAS.er_name AS NAME,
    EVENTS.ev_name AS NAME_1,
    ERAS.er_start_year AS START_YEAR
  FROM ERAS AS ERAS
  JOIN EVENTS AS EVENTS
    ON ERAS.er_end_year > YEAR(EVENTS.ev_dt)
    AND ERAS.er_start_year <= YEAR(EVENTS.ev_dt)
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY ERAS.er_name ORDER BY EVENTS.ev_dt) = 1
)
SELECT
  NAME AS era_name,
  NAME_1 AS event_name
FROM _T1
ORDER BY
  START_YEAR NULLS FIRST
