SELECT
  "sbtransaction"."sbtxdatetime" AS "x",
  CAST('2025-05-02 11:00:00' AS TIMESTAMP) AS "y1",
  CAST('2023-04-03 13:16:30' AS TIMESTAMP) AS "y",
  DATEDIFF(CAST('2025-05-02 11:00:00' AS TIMESTAMP), "sbtransaction"."sbtxdatetime", YEAR) AS "years_diff",
  DATEDIFF(CAST('2025-05-02 11:00:00' AS TIMESTAMP), "sbtransaction"."sbtxdatetime", MONTH) AS "months_diff",
  DATEDIFF(CAST('2025-05-02 11:00:00' AS TIMESTAMP), "sbtransaction"."sbtxdatetime", DAY) AS "days_diff",
  DATEDIFF(CAST('2025-05-02 11:00:00' AS TIMESTAMP), "sbtransaction"."sbtxdatetime", HOUR) AS "hours_diff",
  DATEDIFF(CAST('2023-04-03 13:16:30' AS TIMESTAMP), "sbtransaction"."sbtxdatetime", MINUTE) AS "minutes_diff",
  DATEDIFF(CAST('2023-04-03 13:16:30' AS TIMESTAMP), "sbtransaction"."sbtxdatetime", SECOND) AS "seconds_diff"
FROM "main"."sbtransaction" AS "sbtransaction"
WHERE
  EXTRACT(YEAR FROM "sbtransaction"."sbtxdatetime") < 2025
ORDER BY
  "years_diff"
LIMIT 30
