SELECT
  sbtxdatetime AS date_time,
  DATE_TRUNC(
    'DAY',
    DATE_ADD(
      'DAY',
      (
        (
          (
            DAY_OF_WEEK(CAST(sbtxdatetime AS TIMESTAMP)) % 7
          ) + 2
        ) % 7
      ) * -1,
      CAST(sbtxdatetime AS TIMESTAMP)
    )
  ) AS sow,
  CASE
    WHEN (
      (
        DAY_OF_WEEK(sbtxdatetime) % 7
      ) + 1
    ) = 1
    THEN 'Monday'
    WHEN (
      (
        DAY_OF_WEEK(sbtxdatetime) % 7
      ) + 1
    ) = 2
    THEN 'Tuesday'
    WHEN (
      (
        DAY_OF_WEEK(sbtxdatetime) % 7
      ) + 1
    ) = 3
    THEN 'Wednesday'
    WHEN (
      (
        DAY_OF_WEEK(sbtxdatetime) % 7
      ) + 1
    ) = 4
    THEN 'Thursday'
    WHEN (
      (
        DAY_OF_WEEK(sbtxdatetime) % 7
      ) + 1
    ) = 5
    THEN 'Friday'
    WHEN (
      (
        DAY_OF_WEEK(sbtxdatetime) % 7
      ) + 1
    ) = 6
    THEN 'Saturday'
    WHEN (
      (
        DAY_OF_WEEK(sbtxdatetime) % 7
      ) + 1
    ) = 7
    THEN 'Sunday'
  END AS dayname,
  (
    (
      DAY_OF_WEEK(sbtxdatetime) % 7
    ) + 2
  ) % 7 AS dayofweek
FROM main.sbtransaction
WHERE
  DAY(CAST(sbtxdatetime AS TIMESTAMP)) > 1
  AND YEAR(CAST(sbtxdatetime AS TIMESTAMP)) < 2025
