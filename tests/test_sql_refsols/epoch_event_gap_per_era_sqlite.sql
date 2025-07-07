WITH _t1 AS (
  SELECT
    CAST((
      JULIANDAY(DATE(events.ev_dt, 'start of day')) - JULIANDAY(
        DATE(
          LAG(events.ev_dt, 1) OVER (PARTITION BY eras.er_name ORDER BY events.ev_dt),
          'start of day'
        )
      )
    ) AS INTEGER) AS day_gap,
    eras.er_name,
    eras.er_start_year
  FROM eras AS eras
  JOIN events AS events
    ON eras.er_end_year > CAST(STRFTIME('%Y', events.ev_dt) AS INTEGER)
    AND eras.er_start_year <= CAST(STRFTIME('%Y', events.ev_dt) AS INTEGER)
)
SELECT
  MAX(er_name) AS era_name,
  AVG(day_gap) AS avg_event_gap
FROM _t1
GROUP BY
  er_name
ORDER BY
  MAX(er_start_year)
