WITH _t2 AS (
  SELECT
    eras.er_end_year,
    eras.er_name,
    eras.er_start_year,
    events.ev_dt,
    CAST((
      JULIANDAY(DATE(events.ev_dt, 'start of day')) - JULIANDAY(
        DATE(
          LAG(events.ev_dt, 1) OVER (PARTITION BY eras.er_name, eras.er_name ORDER BY events.ev_dt),
          'start of day'
        )
      )
    ) AS INTEGER) AS day_gap
  FROM eras AS eras
  JOIN events AS events
    ON eras.er_end_year > CAST(STRFTIME('%Y', events.ev_dt) AS INTEGER)
    AND eras.er_start_year <= CAST(STRFTIME('%Y', events.ev_dt) AS INTEGER)
)
SELECT
  er_name AS era_name,
  AVG(day_gap) AS avg_event_gap
FROM _t2
WHERE
  er_end_year > CAST(STRFTIME('%Y', ev_dt) AS INTEGER)
  AND er_start_year <= CAST(STRFTIME('%Y', ev_dt) AS INTEGER)
GROUP BY
  1
ORDER BY
  MAX(er_start_year)
