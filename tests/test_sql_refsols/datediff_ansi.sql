WITH _t0 AS (
  SELECT
    DATEDIFF(
      CAST('2025-05-02 11:00:00' AS TIMESTAMP),
      CAST(sbtransaction.sbtxdatetime AS DATETIME),
      DAY
    ) AS days_diff,
    DATEDIFF(
      CAST('2025-05-02 11:00:00' AS TIMESTAMP),
      CAST(sbtransaction.sbtxdatetime AS DATETIME),
      HOUR
    ) AS hours_diff,
    DATEDIFF(
      CAST('2023-04-03 13:16:30' AS TIMESTAMP),
      CAST(sbtransaction.sbtxdatetime AS DATETIME),
      MINUTE
    ) AS minutes_diff,
    DATEDIFF(
      CAST('2025-05-02 11:00:00' AS TIMESTAMP),
      CAST(sbtransaction.sbtxdatetime AS DATETIME),
      MONTH
    ) AS months_diff,
    DATEDIFF(
      CAST('2025-05-02 11:00:00' AS TIMESTAMP),
      CAST(sbtransaction.sbtxdatetime AS DATETIME),
      YEAR
    ) AS ordering_0,
    DATEDIFF(
      CAST('2023-04-03 13:16:30' AS TIMESTAMP),
      CAST(sbtransaction.sbtxdatetime AS DATETIME),
      SECOND
    ) AS seconds_diff,
    sbtransaction.sbtxdatetime AS x,
    CAST('2023-04-03 13:16:30' AS TIMESTAMP) AS y,
    CAST('2025-05-02 11:00:00' AS TIMESTAMP) AS y1,
    DATEDIFF(
      CAST('2025-05-02 11:00:00' AS TIMESTAMP),
      CAST(sbtransaction.sbtxdatetime AS DATETIME),
      YEAR
    ) AS years_diff
  FROM main.sbtransaction AS sbtransaction
  WHERE
    EXTRACT(YEAR FROM CAST(sbtransaction.sbtxdatetime AS DATETIME)) < 2025
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
