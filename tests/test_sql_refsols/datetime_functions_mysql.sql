SELECT
  CURRENT_TIMESTAMP() AS ts_now_1,
  CAST(CURRENT_TIMESTAMP() AS DATE) AS ts_now_2,
  STR_TO_DATE(
    CONCAT(YEAR(CURRENT_TIMESTAMP()), ' ', MONTH(CURRENT_TIMESTAMP()), ' 1'),
    '%Y %c %e'
  ) AS ts_now_3,
  DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL '1' HOUR) AS ts_now_4,
  STR_TO_DATE(
    CONCAT(
      YEAR(CAST('2025-01-01 00:00:00' AS DATETIME)),
      ' ',
      MONTH(CAST('2025-01-01 00:00:00' AS DATETIME)),
      ' 1'
    ),
    '%Y %c %e'
  ) AS ts_now_5,
  CAST('1995-10-08 00:00:00' AS DATETIME) AS ts_now_6,
  EXTRACT(YEAR FROM CAST(o_orderdate AS DATETIME)) AS year_col,
  2020 AS year_py,
  1995 AS year_pd,
  EXTRACT(MONTH FROM CAST(o_orderdate AS DATETIME)) AS month_col,
  2 AS month_str,
  1 AS month_dt,
  EXTRACT(DAY FROM CAST(o_orderdate AS DATETIME)) AS day_col,
  25 AS day_str,
  23 AS hour_str,
  59 AS minute_str,
  59 AS second_ts,
  DATEDIFF(CAST('1992-01-01' AS DATETIME), o_orderdate) AS dd_col_str,
  DATEDIFF(o_orderdate, CAST('1992-01-01' AS DATETIME)) AS dd_str_col,
  (
    YEAR(o_orderdate) - YEAR(CAST('1995-10-10 00:00:00' AS DATETIME))
  ) * 12 + (
    MONTH(o_orderdate) - MONTH(CAST('1995-10-10 00:00:00' AS DATETIME))
  ) AS dd_pd_col,
  YEAR(CAST('1992-01-01 12:30:45' AS DATETIME)) - YEAR(o_orderdate) AS dd_col_dt,
  CAST((
    DATEDIFF(CAST('1992-01-01 12:30:45' AS DATETIME), CAST('1992-01-01' AS DATETIME)) + (
      (
        DAYOFWEEK(CAST('1992-01-01' AS DATETIME)) + -1
      ) % 7
    ) - (
      (
        DAYOFWEEK(CAST('1992-01-01 12:30:45' AS DATETIME)) + -1
      ) % 7
    )
  ) / 7 AS SIGNED) AS dd_dt_str,
  (
    DAYOFWEEK(o_orderdate) + -1
  ) % 7 AS dow_col,
  (
    DAYOFWEEK(CAST('1992-07-01' AS DATE)) + -1
  ) % 7 AS dow_str,
  (
    DAYOFWEEK(CAST('1992-01-01 12:30:45' AS DATETIME)) + -1
  ) % 7 AS dow_dt,
  (
    DAYOFWEEK(CAST('1995-10-10 00:00:00' AS DATETIME)) + -1
  ) % 7 AS dow_pd,
  DAYNAME(o_orderdate) AS dayname_col,
  DAYNAME('1995-06-30') AS dayname_str,
  DAYNAME(CAST('1993-08-15 00:00:00' AS DATETIME)) AS dayname_dt
FROM tpch.ORDERS
