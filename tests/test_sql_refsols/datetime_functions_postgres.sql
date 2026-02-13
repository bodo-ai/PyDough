SELECT
  CURRENT_TIMESTAMP AS ts_now_1,
  DATE_TRUNC('DAY', CURRENT_TIMESTAMP) AS ts_now_2,
  DATE_TRUNC('MONTH', CURRENT_TIMESTAMP) AS ts_now_3,
  CURRENT_TIMESTAMP + INTERVAL '1 HOUR' AS ts_now_4,
  CAST('2025-01-01' AS DATE) AS ts_now_5,
  CAST('1995-10-08' AS DATE) AS ts_now_6,
  EXTRACT(YEAR FROM CAST(o_orderdate AS TIMESTAMP)) AS year_col,
  2020 AS year_py,
  1995 AS year_pd,
  EXTRACT(MONTH FROM CAST(o_orderdate AS TIMESTAMP)) AS month_col,
  2 AS month_str,
  1 AS month_dt,
  EXTRACT(DAY FROM CAST(o_orderdate AS TIMESTAMP)) AS day_col,
  25 AS day_str,
  23 AS hour_str,
  59 AS minute_str,
  59 AS second_ts,
  CAST(CAST('1992-01-01' AS TIMESTAMP) AS DATE) - CAST(o_orderdate AS DATE) AS dd_col_str,
  CAST(o_orderdate AS DATE) - CAST(CAST('1992-01-01' AS TIMESTAMP) AS DATE) AS dd_str_col,
  (
    EXTRACT(YEAR FROM CAST(o_orderdate AS TIMESTAMP)) - EXTRACT(YEAR FROM CAST('1995-10-10 00:00:00' AS TIMESTAMP))
  ) * 12 + (
    EXTRACT(MONTH FROM CAST(o_orderdate AS TIMESTAMP)) - EXTRACT(MONTH FROM CAST('1995-10-10 00:00:00' AS TIMESTAMP))
  ) AS dd_pd_col,
  EXTRACT(YEAR FROM CAST('1992-01-01 12:30:45' AS TIMESTAMP)) - EXTRACT(YEAR FROM CAST(o_orderdate AS TIMESTAMP)) AS dd_col_dt,
  CAST(CAST((
    CAST(CAST('1992-01-01 12:30:45' AS TIMESTAMP) AS DATE) - CAST(CAST('1992-01-01' AS TIMESTAMP) AS DATE) + EXTRACT(DOW FROM CAST('1992-01-01' AS TIMESTAMP)) - EXTRACT(DOW FROM CAST('1992-01-01 12:30:45' AS TIMESTAMP))
  ) AS DOUBLE PRECISION) / 7 AS BIGINT) AS dd_dt_str,
  EXTRACT(DOW FROM CAST(o_orderdate AS TIMESTAMP)) AS dow_col,
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
    WHEN EXTRACT(DOW FROM CAST(o_orderdate AS TIMESTAMP)) = 0
    THEN 'Sunday'
    WHEN EXTRACT(DOW FROM CAST(o_orderdate AS TIMESTAMP)) = 1
    THEN 'Monday'
    WHEN EXTRACT(DOW FROM CAST(o_orderdate AS TIMESTAMP)) = 2
    THEN 'Tuesday'
    WHEN EXTRACT(DOW FROM CAST(o_orderdate AS TIMESTAMP)) = 3
    THEN 'Wednesday'
    WHEN EXTRACT(DOW FROM CAST(o_orderdate AS TIMESTAMP)) = 4
    THEN 'Thursday'
    WHEN EXTRACT(DOW FROM CAST(o_orderdate AS TIMESTAMP)) = 5
    THEN 'Friday'
    WHEN EXTRACT(DOW FROM CAST(o_orderdate AS TIMESTAMP)) = 6
    THEN 'Saturday'
  END AS dayname_col,
  'Monday' AS dayname_str1,
  'Tuesday' AS dayname_str2,
  'Wednesday' AS dayname_str3,
  'Thursday' AS dayname_str4,
  'Friday' AS dayname_str5,
  'Saturday' AS dayname_str6,
  'Sunday' AS dayname_dt
FROM tpch.orders
