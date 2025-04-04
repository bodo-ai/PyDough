SELECT
  "sbtransaction"."sbtxdatetime" AS "x",
  '2025-05-02 11:00:00' AS "y1",
  '2023-04-03 13:16:30' AS "y",
  CAST(STRFTIME('%Y', '2025-05-02 11:00:00') AS INTEGER) - CAST(STRFTIME('%Y', "sbtransaction"."sbtxdatetime") AS INTEGER) AS "years_diff",
  (
    CAST(STRFTIME('%Y', '2025-05-02 11:00:00') AS INTEGER) - CAST(STRFTIME('%Y', "sbtransaction"."sbtxdatetime") AS INTEGER)
  ) * 12 + CAST(STRFTIME('%m', '2025-05-02 11:00:00') AS INTEGER) - CAST(STRFTIME('%m', "sbtransaction"."sbtxdatetime") AS INTEGER) AS "months_diff",
  CAST((
    JULIANDAY(DATE('2025-05-02 11:00:00', 'start of day')) - JULIANDAY(DATE("sbtransaction"."sbtxdatetime", 'start of day'))
  ) AS INTEGER) AS "days_diff",
  CAST((
    JULIANDAY(DATE('2025-05-02 11:00:00', 'start of day')) - JULIANDAY(DATE("sbtransaction"."sbtxdatetime", 'start of day'))
  ) AS INTEGER) * 24 + CAST(STRFTIME('%H', '2025-05-02 11:00:00') AS INTEGER) - CAST(STRFTIME('%H', "sbtransaction"."sbtxdatetime") AS INTEGER) AS "hours_diff",
  (
    CAST((
      JULIANDAY(DATE('2023-04-03 13:16:30', 'start of day')) - JULIANDAY(DATE("sbtransaction"."sbtxdatetime", 'start of day'))
    ) AS INTEGER) * 24 + CAST(STRFTIME('%H', '2023-04-03 13:16:30') AS INTEGER) - CAST(STRFTIME('%H', "sbtransaction"."sbtxdatetime") AS INTEGER)
  ) * 60 + CAST(STRFTIME('%M', '2023-04-03 13:16:30') AS INTEGER) - CAST(STRFTIME('%M', "sbtransaction"."sbtxdatetime") AS INTEGER) AS "minutes_diff",
  (
    (
      CAST((
        JULIANDAY(DATE('2023-04-03 13:16:30', 'start of day')) - JULIANDAY(DATE("sbtransaction"."sbtxdatetime", 'start of day'))
      ) AS INTEGER) * 24 + CAST(STRFTIME('%H', '2023-04-03 13:16:30') AS INTEGER) - CAST(STRFTIME('%H', "sbtransaction"."sbtxdatetime") AS INTEGER)
    ) * 60 + CAST(STRFTIME('%M', '2023-04-03 13:16:30') AS INTEGER) - CAST(STRFTIME('%M', "sbtransaction"."sbtxdatetime") AS INTEGER)
  ) * 60 + CAST(STRFTIME('%S', '2023-04-03 13:16:30') AS INTEGER) - CAST(STRFTIME('%S', "sbtransaction"."sbtxdatetime") AS INTEGER) AS "seconds_diff"
FROM "main"."sbtransaction" AS "sbtransaction"
WHERE
  CAST(STRFTIME('%Y', "sbtransaction"."sbtxdatetime") AS INTEGER) < 2025
ORDER BY
  "years_diff"
LIMIT 30
