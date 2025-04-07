WITH _t0 AS (
  SELECT
    CAST((
      JULIANDAY(DATE('2025-05-02 11:00:00', 'start of day')) - JULIANDAY(DATE(sbtxdatetime, 'start of day'))
    ) AS INTEGER) AS days_diff,
    CAST((
      JULIANDAY(DATE('2025-05-02 11:00:00', 'start of day')) - JULIANDAY(DATE(sbtxdatetime, 'start of day'))
    ) AS INTEGER) * 24 + CAST(STRFTIME('%H', '2025-05-02 11:00:00') AS INTEGER) - CAST(STRFTIME('%H', sbtxdatetime) AS INTEGER) AS hours_diff,
    (
      CAST((
        JULIANDAY(DATE('2023-04-03 13:16:30', 'start of day')) - JULIANDAY(DATE(sbtxdatetime, 'start of day'))
      ) AS INTEGER) * 24 + CAST(STRFTIME('%H', '2023-04-03 13:16:30') AS INTEGER) - CAST(STRFTIME('%H', sbtxdatetime) AS INTEGER)
    ) * 60 + CAST(STRFTIME('%M', '2023-04-03 13:16:30') AS INTEGER) - CAST(STRFTIME('%M', sbtxdatetime) AS INTEGER) AS minutes_diff,
    (
      CAST(STRFTIME('%Y', '2025-05-02 11:00:00') AS INTEGER) - CAST(STRFTIME('%Y', sbtxdatetime) AS INTEGER)
    ) * 12 + CAST(STRFTIME('%m', '2025-05-02 11:00:00') AS INTEGER) - CAST(STRFTIME('%m', sbtxdatetime) AS INTEGER) AS months_diff,
    CAST(STRFTIME('%Y', '2025-05-02 11:00:00') AS INTEGER) - CAST(STRFTIME('%Y', sbtxdatetime) AS INTEGER) AS ordering_0,
    (
      (
        CAST((
          JULIANDAY(DATE('2023-04-03 13:16:30', 'start of day')) - JULIANDAY(DATE(sbtxdatetime, 'start of day'))
        ) AS INTEGER) * 24 + CAST(STRFTIME('%H', '2023-04-03 13:16:30') AS INTEGER) - CAST(STRFTIME('%H', sbtxdatetime) AS INTEGER)
      ) * 60 + CAST(STRFTIME('%M', '2023-04-03 13:16:30') AS INTEGER) - CAST(STRFTIME('%M', sbtxdatetime) AS INTEGER)
    ) * 60 + CAST(STRFTIME('%S', '2023-04-03 13:16:30') AS INTEGER) - CAST(STRFTIME('%S', sbtxdatetime) AS INTEGER) AS seconds_diff,
    sbtxdatetime AS x,
    '2023-04-03 13:16:30' AS y,
    '2025-05-02 11:00:00' AS y1,
    CAST(STRFTIME('%Y', '2025-05-02 11:00:00') AS INTEGER) - CAST(STRFTIME('%Y', sbtxdatetime) AS INTEGER) AS years_diff
  FROM main.sbtransaction
  WHERE
    CAST(STRFTIME('%Y', sbtxdatetime) AS INTEGER) < 2025
  ORDER BY
    ordering_0
  LIMIT 30
)
SELECT
  x,
  y1,
  y,
  years_diff,
  months_diff,
  days_diff,
  hours_diff,
  minutes_diff,
  seconds_diff
FROM _t0
ORDER BY
  ordering_0
