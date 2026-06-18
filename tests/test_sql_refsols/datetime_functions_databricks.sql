SELECT
  CURRENT_TIMESTAMP() AS ts_now_1,
  DATE_TRUNC('DAY', CURRENT_TIMESTAMP()) AS ts_now_2,
  TRUNC(CURRENT_TIMESTAMP(), 'MONTH') AS ts_now_3,
  CURRENT_TIMESTAMP() + INTERVAL '1' HOUR AS ts_now_4,
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
  DATEDIFF(DAY, CAST(o_orderdate AS DATE), CAST(CAST('1992-01-01' AS TIMESTAMP) AS DATE)) AS dd_col_str,
  DATEDIFF(DAY, CAST(CAST('1992-01-01' AS TIMESTAMP) AS DATE), CAST(o_orderdate AS DATE)) AS dd_str_col,
  (
    YEAR(TO_DATE(o_orderdate)) - YEAR(TO_DATE(CAST('1995-10-10 00:00:00' AS TIMESTAMP)))
  ) * 12 + MONTH(TO_DATE(o_orderdate)) - MONTH(TO_DATE(CAST('1995-10-10 00:00:00' AS TIMESTAMP))) AS dd_pd_col,
  YEAR(TO_DATE(CAST('1992-01-01 12:30:45' AS TIMESTAMP))) - YEAR(TO_DATE(o_orderdate)) AS dd_col_dt,
  CAST(DATEDIFF(
    DAY,
    DATEADD(
      DAY,
      -(
        (
          DAYOFWEEK(TO_DATE(CAST('1992-01-01' AS TIMESTAMP))) + -1
        ) % 7
      ),
      CAST(CAST('1992-01-01' AS TIMESTAMP) AS DATE)
    ),
    DATEADD(
      DAY,
      -(
        (
          DAYOFWEEK(TO_DATE(CAST('1992-01-01 12:30:45' AS TIMESTAMP))) + -1
        ) % 7
      ),
      CAST(CAST('1992-01-01 12:30:45' AS TIMESTAMP) AS DATE)
    )
  ) / 7 AS BIGINT) AS dd_dt_str,
  (
    DAYOFWEEK(TO_DATE(o_orderdate)) + -1
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
    WHEN DAYOFWEEK(TO_DATE(o_orderdate)) = 1
    THEN 'Sunday'
    WHEN DAYOFWEEK(TO_DATE(o_orderdate)) = 2
    THEN 'Monday'
    WHEN DAYOFWEEK(TO_DATE(o_orderdate)) = 3
    THEN 'Tuesday'
    WHEN DAYOFWEEK(TO_DATE(o_orderdate)) = 4
    THEN 'Wednesday'
    WHEN DAYOFWEEK(TO_DATE(o_orderdate)) = 5
    THEN 'Thursday'
    WHEN DAYOFWEEK(TO_DATE(o_orderdate)) = 6
    THEN 'Friday'
    WHEN DAYOFWEEK(TO_DATE(o_orderdate)) = 7
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
