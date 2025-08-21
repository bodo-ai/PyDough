SELECT
  sbtxdatetime AS x,
  CAST('2025-05-02 11:00:00' AS DATETIME) AS y1,
  CAST('2023-04-03 13:16:30' AS DATETIME) AS y,
  YEAR(CAST('2025-05-02 11:00:00' AS DATETIME)) - YEAR(sbtxdatetime) AS years_diff,
  (
    YEAR(CAST('2025-05-02 11:00:00' AS DATETIME)) - YEAR(sbtxdatetime)
  ) * 12 + (
    MONTH(CAST('2025-05-02 11:00:00' AS DATETIME)) - MONTH(sbtxdatetime)
  ) AS months_diff,
  DATEDIFF(CAST('2025-05-02 11:00:00' AS DATETIME), sbtxdatetime) AS days_diff,
  TIMESTAMPDIFF(
    HOUR,
    DATE_FORMAT(sbtxdatetime, '%Y-%m-%d %H:00:00'),
    DATE_FORMAT(CAST('2025-05-02 11:00:00' AS DATETIME), '%Y-%m-%d %H:00:00')
  ) AS hours_diff,
  TIMESTAMPDIFF(
    MINUTE,
    DATE_FORMAT(sbtxdatetime, '%Y-%m-%d %H:%i:00'),
    DATE_FORMAT(CAST('2023-04-03 13:16:30' AS DATETIME), '%Y-%m-%d %H:%i:00')
  ) AS minutes_diff,
  TIMESTAMPDIFF(SECOND, sbtxdatetime, CAST('2023-04-03 13:16:30' AS DATETIME)) AS seconds_diff
FROM main.sbTransaction
WHERE
  EXTRACT(YEAR FROM CAST(sbtxdatetime AS DATETIME)) < 2025
ORDER BY
  4
LIMIT 30
