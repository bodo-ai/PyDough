SELECT
  CURRENT_TIMESTAMP AS ts_now_1,
  DATE_TRUNC('DAY', CURRENT_TIMESTAMP) AS ts_now_2,
  DATE_TRUNC('MONTH', CURRENT_TIMESTAMP) AS ts_now_3,
  DATE_ADD('HOUR', 1, CURRENT_TIMESTAMP) AS ts_now_4,
  CAST('2025-01-01' AS DATE) AS ts_now_5,
  CAST('1995-10-08' AS DATE) AS ts_now_6,
  YEAR(CAST(o_orderdate AS TIMESTAMP)) AS year_col,
  2020 AS year_py,
  1995 AS year_pd,
  MONTH(CAST(o_orderdate AS TIMESTAMP)) AS month_col,
  2 AS month_str,
  1 AS month_dt,
  DAY(CAST(o_orderdate AS TIMESTAMP)) AS day_col,
  25 AS day_str,
  23 AS hour_str,
  59 AS minute_str,
  59 AS second_ts,
  DATE_DIFF(
    'DAY',
    CAST(DATE_TRUNC('DAY', CAST(o_orderdate AS TIMESTAMP)) AS TIMESTAMP),
    CAST(DATE_TRUNC('DAY', CAST('1992-01-01' AS TIMESTAMP)) AS TIMESTAMP)
  ) AS dd_col_str,
  DATE_DIFF(
    'DAY',
    CAST(DATE_TRUNC('DAY', CAST('1992-01-01' AS TIMESTAMP)) AS TIMESTAMP),
    CAST(DATE_TRUNC('DAY', CAST(o_orderdate AS TIMESTAMP)) AS TIMESTAMP)
  ) AS dd_str_col,
  DATE_DIFF(
    'MONTH',
    CAST(DATE_TRUNC('MONTH', CAST('1995-10-10 00:00:00' AS TIMESTAMP)) AS TIMESTAMP),
    CAST(DATE_TRUNC('MONTH', CAST(o_orderdate AS TIMESTAMP)) AS TIMESTAMP)
  ) AS dd_pd_col,
  DATE_DIFF(
    'YEAR',
    CAST(DATE_TRUNC('YEAR', CAST(o_orderdate AS TIMESTAMP)) AS TIMESTAMP),
    CAST(DATE_TRUNC('YEAR', CAST('1992-01-01 12:30:45' AS TIMESTAMP)) AS TIMESTAMP)
  ) AS dd_col_dt,
  DATE_DIFF(
    'WEEK',
    CAST(DATE_TRUNC(
      'DAY',
      DATE_ADD(
        'DAY',
        (
          (
            DAY_OF_WEEK(CAST('1992-01-01' AS TIMESTAMP)) - 7
          ) % 7
        ) * -1,
        CAST('1992-01-01' AS TIMESTAMP)
      )
    ) AS TIMESTAMP),
    CAST(DATE_TRUNC(
      'DAY',
      DATE_ADD(
        'DAY',
        (
          (
            DAY_OF_WEEK(CAST('1992-01-01 12:30:45' AS TIMESTAMP)) - 7
          ) % 7
        ) * -1,
        CAST('1992-01-01 12:30:45' AS TIMESTAMP)
      )
    ) AS TIMESTAMP)
  ) AS dd_dt_str,
  (
    DAY_OF_WEEK(o_orderdate) - 7
  ) % 7 AS dow_col,
  3 AS dow_str1,
  4 AS dow_str2,
  5 AS dow_str3,
  6 AS dow_str4,
  0 AS dow_str5,
  1 AS dow_str6,
  2 AS dow_str7,
  3 AS dow_dt,
  2 AS dow_pd,
  CASE
    WHEN DAY_OF_WEEK(o_orderdate) = 1
    THEN 'Monday'
    WHEN DAY_OF_WEEK(o_orderdate) = 2
    THEN 'Tuesday'
    WHEN DAY_OF_WEEK(o_orderdate) = 3
    THEN 'Wednesday'
    WHEN DAY_OF_WEEK(o_orderdate) = 4
    THEN 'Thursday'
    WHEN DAY_OF_WEEK(o_orderdate) = 5
    THEN 'Friday'
    WHEN DAY_OF_WEEK(o_orderdate) = 6
    THEN 'Saturday'
    WHEN DAY_OF_WEEK(o_orderdate) = 7
    THEN 'Sunday'
  END AS dayname_col,
  'Monday' AS dayname_str1,
  'Tuesday' AS dayname_str2,
  'Wednesday' AS dayname_str3,
  'Thursday' AS dayname_str4,
  'Friday' AS dayname_str5,
  'Saturday' AS dayname_str6,
  'Sunday' AS dayname_dt
FROM tpch.orders
