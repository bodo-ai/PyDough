SELECT
  CURRENT_TIMESTAMP() AS ts_now_1,
  DATE_TRUNC('DAY', CURRENT_TIMESTAMP()) AS ts_now_2,
  DATE_TRUNC('MONTH', CURRENT_TIMESTAMP()) AS ts_now_3,
  DATE_ADD(CURRENT_TIMESTAMP(), 1, 'HOUR') AS ts_now_4,
  CAST('2025-01-01 00:00:00' AS TIMESTAMP) AS ts_now_5,
  CAST('1995-10-08 00:00:00' AS TIMESTAMP) AS ts_now_6,
  EXTRACT(YEAR FROM CAST(o_orderdate AS DATETIME)) AS year_col,
  EXTRACT(YEAR FROM CAST('2020-05-01 00:00:00' AS TIMESTAMP)) AS year_py,
  EXTRACT(YEAR FROM CAST('1995-10-10 00:00:00' AS TIMESTAMP)) AS year_pd,
  EXTRACT(MONTH FROM CAST(o_orderdate AS DATETIME)) AS month_col,
  EXTRACT(MONTH FROM CAST('2025-02-25' AS TIMESTAMP)) AS month_str,
  EXTRACT(MONTH FROM CAST('1992-01-01 12:30:45' AS TIMESTAMP)) AS month_dt,
  EXTRACT(DAY FROM CAST(o_orderdate AS DATETIME)) AS day_col,
  EXTRACT(DAY FROM CAST('1996-11-25 10:45:00' AS TIMESTAMP)) AS day_str,
  EXTRACT(HOUR FROM CAST('1995-12-01 23:59:59' AS TIMESTAMP)) AS hour_str,
  EXTRACT(MINUTE FROM CAST('1995-12-01 23:59:59' AS TIMESTAMP)) AS minute_str,
  EXTRACT(SECOND FROM CAST('1992-01-01 00:00:59' AS TIMESTAMP)) AS second_ts,
  DATEDIFF(CAST('1992-01-01' AS TIMESTAMP), CAST(o_orderdate AS DATETIME), DAY) AS dd_col_str,
  DATEDIFF(CAST(o_orderdate AS DATETIME), CAST('1992-01-01' AS TIMESTAMP), DAY) AS dd_str_col,
  DATEDIFF(CAST(o_orderdate AS DATETIME), CAST('1995-10-10 00:00:00' AS TIMESTAMP), MONTH) AS dd_pd_col,
  DATEDIFF(CAST('1992-01-01 12:30:45' AS TIMESTAMP), CAST(o_orderdate AS DATETIME), YEAR) AS dd_col_dt,
  DATEDIFF(CAST('1992-01-01 12:30:45' AS TIMESTAMP), CAST('1992-01-01' AS TIMESTAMP), WEEK) AS dd_dt_str,
  DAY_OF_WEEK(o_orderdate) AS dow_col,
  DAY_OF_WEEK('1992-07-01') AS dow_str,
  DAY_OF_WEEK(CAST('1992-01-01 12:30:45' AS TIMESTAMP)) AS dow_dt,
  DAY_OF_WEEK(CAST('1995-10-10 00:00:00' AS TIMESTAMP)) AS dow_pd,
  CASE
    WHEN DAY_OF_WEEK(o_orderdate) = 0
    THEN 'Sunday'
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
  END AS dayname_col,
  CASE
    WHEN DAY_OF_WEEK('1995-06-30') = 0
    THEN 'Sunday'
    WHEN DAY_OF_WEEK('1995-06-30') = 1
    THEN 'Monday'
    WHEN DAY_OF_WEEK('1995-06-30') = 2
    THEN 'Tuesday'
    WHEN DAY_OF_WEEK('1995-06-30') = 3
    THEN 'Wednesday'
    WHEN DAY_OF_WEEK('1995-06-30') = 4
    THEN 'Thursday'
    WHEN DAY_OF_WEEK('1995-06-30') = 5
    THEN 'Friday'
    WHEN DAY_OF_WEEK('1995-06-30') = 6
    THEN 'Saturday'
  END AS dayname_str,
  CASE
    WHEN DAY_OF_WEEK(CAST('1993-08-15 00:00:00' AS TIMESTAMP)) = 0
    THEN 'Sunday'
    WHEN DAY_OF_WEEK(CAST('1993-08-15 00:00:00' AS TIMESTAMP)) = 1
    THEN 'Monday'
    WHEN DAY_OF_WEEK(CAST('1993-08-15 00:00:00' AS TIMESTAMP)) = 2
    THEN 'Tuesday'
    WHEN DAY_OF_WEEK(CAST('1993-08-15 00:00:00' AS TIMESTAMP)) = 3
    THEN 'Wednesday'
    WHEN DAY_OF_WEEK(CAST('1993-08-15 00:00:00' AS TIMESTAMP)) = 4
    THEN 'Thursday'
    WHEN DAY_OF_WEEK(CAST('1993-08-15 00:00:00' AS TIMESTAMP)) = 5
    THEN 'Friday'
    WHEN DAY_OF_WEEK(CAST('1993-08-15 00:00:00' AS TIMESTAMP)) = 6
    THEN 'Saturday'
  END AS dayname_dt
FROM tpch.orders
