SELECT
  sbtxdatetime AS date_time,
  DATE_TRUNC('WEEK', CAST(sbtxdatetime AS TIMESTAMP)) AS sow,
  DAYNAME(sbtxdatetime) AS dayname,
  DAYOFWEEK(sbtxdatetime) AS dayofweek
FROM MAIN.SBTRANSACTION
WHERE
  DAY(sbtxdatetime) > 1 AND YEAR(sbtxdatetime) < 2025
