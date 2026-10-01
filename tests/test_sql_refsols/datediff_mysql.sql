SELECT
  sbTxDateTime AS x,
  CAST('2025-05-02 11:00:00' AS DATETIME) AS y1,
  CAST('2023-04-03 13:16:30' AS DATETIME) AS y,
  YEAR(CAST('2025-05-02 11:00:00' AS DATETIME)) - YEAR(sbTxDateTime) AS years_diff,
  (
    YEAR(CAST('2025-05-02 11:00:00' AS DATETIME)) - YEAR(sbTxDateTime)
  ) * 12 + (
    MONTH(CAST('2025-05-02 11:00:00' AS DATETIME)) - MONTH(sbTxDateTime)
  ) AS months_diff,
  DATEDIFF(CAST('2025-05-02 11:00:00' AS DATETIME), sbTxDateTime) AS days_diff,
  TIMESTAMPDIFF(
    HOUR,
    DATE_FORMAT(sbTxDateTime, '%Y-%m-%d %H:00:00'),
    DATE_FORMAT(CAST('2025-05-02 11:00:00' AS DATETIME), '%Y-%m-%d %H:00:00')
  ) AS hours_diff,
  TIMESTAMPDIFF(
    MINUTE,
    DATE_FORMAT(sbTxDateTime, '%Y-%m-%d %H:%i:00'),
    DATE_FORMAT(CAST('2023-04-03 13:16:30' AS DATETIME), '%Y-%m-%d %H:%i:00')
  ) AS minutes_diff,
  TIMESTAMPDIFF(SECOND, sbTxDateTime, CAST('2023-04-03 13:16:30' AS DATETIME)) AS seconds_diff
FROM main.sbTransaction
WHERE
  EXTRACT(YEAR FROM CAST(sbTxDateTime AS DATETIME)) < 2025
ORDER BY
  4
LIMIT 30
