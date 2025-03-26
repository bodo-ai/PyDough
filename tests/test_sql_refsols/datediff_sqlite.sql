WITH _t0 AS (
  SELECT
    CAST((
      JULIANDAY(DATE('2025-05-02 11:00:00', 'start of day')) - JULIANDAY(DATE(sbtransaction.sbtxdatetime, 'start of day'))
    ) AS INTEGER) AS days_diff,
    CAST((
      JULIANDAY(DATE('2025-05-02 11:00:00', 'start of day')) - JULIANDAY(DATE(sbtransaction.sbtxdatetime, 'start of day'))
    ) AS INTEGER) * 24 + CAST(STRFTIME('%H', '2025-05-02 11:00:00') AS INTEGER) - CAST(STRFTIME('%H', sbtransaction.sbtxdatetime) AS INTEGER) AS hours_diff,
    (
      CAST((
        JULIANDAY(DATE('2023-04-03 13:16:30', 'start of day')) - JULIANDAY(DATE(sbtransaction.sbtxdatetime, 'start of day'))
      ) AS INTEGER) * 24 + CAST(STRFTIME('%H', '2023-04-03 13:16:30') AS INTEGER) - CAST(STRFTIME('%H', sbtransaction.sbtxdatetime) AS INTEGER)
    ) * 60 + CAST(STRFTIME('%M', '2023-04-03 13:16:30') AS INTEGER) - CAST(STRFTIME('%M', sbtransaction.sbtxdatetime) AS INTEGER) AS minutes_diff,
    (
      CAST(STRFTIME('%Y', '2025-05-02 11:00:00') AS INTEGER) - CAST(STRFTIME('%Y', sbtransaction.sbtxdatetime) AS INTEGER)
    ) * 12 + CAST(STRFTIME('%m', '2025-05-02 11:00:00') AS INTEGER) - CAST(STRFTIME('%m', sbtransaction.sbtxdatetime) AS INTEGER) AS months_diff,
    CAST(STRFTIME('%Y', '2025-05-02 11:00:00') AS INTEGER) - CAST(STRFTIME('%Y', sbtransaction.sbtxdatetime) AS INTEGER) AS ordering_0,
    (
      (
        CAST((
          JULIANDAY(DATE('2023-04-03 13:16:30', 'start of day')) - JULIANDAY(DATE(sbtransaction.sbtxdatetime, 'start of day'))
        ) AS INTEGER) * 24 + CAST(STRFTIME('%H', '2023-04-03 13:16:30') AS INTEGER) - CAST(STRFTIME('%H', sbtransaction.sbtxdatetime) AS INTEGER)
      ) * 60 + CAST(STRFTIME('%M', '2023-04-03 13:16:30') AS INTEGER) - CAST(STRFTIME('%M', sbtransaction.sbtxdatetime) AS INTEGER)
    ) * 60 + CAST(STRFTIME('%S', '2023-04-03 13:16:30') AS INTEGER) - CAST(STRFTIME('%S', sbtransaction.sbtxdatetime) AS INTEGER) AS seconds_diff,
    sbtransaction.sbtxdatetime AS x,
    '2023-04-03 13:16:30' AS y,
    '2025-05-02 11:00:00' AS y1,
    CAST(STRFTIME('%Y', '2025-05-02 11:00:00') AS INTEGER) - CAST(STRFTIME('%Y', sbtransaction.sbtxdatetime) AS INTEGER) AS years_diff
  FROM main.sbtransaction AS sbtransaction
  WHERE
    CAST(STRFTIME('%Y', sbtransaction.sbtxdatetime) AS INTEGER) < 2025
  ORDER BY
    ordering_0
  LIMIT 30
)
SELECT
  _t0.x AS x,
  _t0.y1 AS y1,
  _t0.y AS y,
  _t0.years_diff AS years_diff,
  _t0.months_diff AS months_diff,
  _t0.days_diff AS days_diff,
  _t0.hours_diff AS hours_diff,
  _t0.minutes_diff AS minutes_diff,
  _t0.seconds_diff AS seconds_diff
FROM _t0 AS _t0
ORDER BY
  _t0.ordering_0
