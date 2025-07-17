SELECT
  sbtxdatetime AS date_time,
  STR_TO_DATE(
    CONCAT(
      YEAR(CAST(sbtxdatetime AS DATETIME)),
      ' ',
      WEEK(CAST(sbtxdatetime AS DATETIME), 1),
      ' 1'
    ),
    '%Y %u %w'
  ) AS sow,
  DAYNAME(sbtxdatetime) AS dayname,
  (
    (
      DAYOFWEEK(sbtxdatetime) + 1
    ) % 7
  ) + 1 AS dayofweek
FROM main.sbTransaction
WHERE
  DAY(sbtxdatetime) > 1 AND YEAR(sbtxdatetime) < 2025
