WITH _t0 AS (
  SELECT
    DATEDIFF(CAST('2025-05-02 11:00:00' AS TIMESTAMP), sbtxdatetime, DAY) AS days_diff,
    DATEDIFF(CAST('2025-05-02 11:00:00' AS TIMESTAMP), sbtxdatetime, HOUR) AS hours_diff,
    DATEDIFF(CAST('2023-04-03 13:16:30' AS TIMESTAMP), sbtxdatetime, MINUTE) AS minutes_diff,
    DATEDIFF(CAST('2025-05-02 11:00:00' AS TIMESTAMP), sbtxdatetime, MONTH) AS months_diff,
    DATEDIFF(CAST('2025-05-02 11:00:00' AS TIMESTAMP), sbtxdatetime, YEAR) AS ordering_0,
    DATEDIFF(CAST('2023-04-03 13:16:30' AS TIMESTAMP), sbtxdatetime, SECOND) AS seconds_diff,
    sbtxdatetime AS x,
    CAST('2023-04-03 13:16:30' AS TIMESTAMP) AS y,
    CAST('2025-05-02 11:00:00' AS TIMESTAMP) AS y1,
    DATEDIFF(CAST('2025-05-02 11:00:00' AS TIMESTAMP), sbtxdatetime, YEAR) AS years_diff
  FROM main.sbtransaction
  WHERE
    EXTRACT(YEAR FROM sbtxdatetime) < 2025
  ORDER BY
    ordering_0
  LIMIT 30
)
SELECT
  x AS x,
  y1 AS y1,
  y AS y,
  years_diff AS years_diff,
  months_diff AS months_diff,
  days_diff AS days_diff,
  hours_diff AS hours_diff,
  minutes_diff AS minutes_diff,
  seconds_diff AS seconds_diff
FROM _t0
ORDER BY
  ordering_0
