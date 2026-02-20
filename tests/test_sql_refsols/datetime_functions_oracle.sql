SELECT
  SYS_EXTRACT_UTC(SYSTIMESTAMP) AS ts_now_1,
  TRUNC(SYS_EXTRACT_UTC(SYSTIMESTAMP), 'DAY') AS ts_now_2,
  TRUNC(SYS_EXTRACT_UTC(SYSTIMESTAMP), 'MONTH') AS ts_now_3,
  SYS_EXTRACT_UTC(SYSTIMESTAMP) + NUMTODSINTERVAL(1, 'hour') AS ts_now_4,
  TO_DATE('2025-01-01', 'YYYY-MM-DD') AS ts_now_5,
  TO_DATE('1995-10-08', 'YYYY-MM-DD') AS ts_now_6,
  EXTRACT(YEAR FROM CAST(o_orderdate AS DATE)) AS year_col,
  2020 AS year_py,
  1995 AS year_pd,
  EXTRACT(MONTH FROM CAST(o_orderdate AS DATE)) AS month_col,
  2 AS month_str,
  1 AS month_dt,
  EXTRACT(DAY FROM CAST(o_orderdate AS DATE)) AS day_col,
  25 AS day_str,
  23 AS hour_str,
  59 AS minute_str,
  59 AS second_ts,
  CAST(CAST('1992-01-01' AS TIMESTAMP) AS DATE) - CAST(o_orderdate AS DATE) AS dd_col_str,
  CAST(o_orderdate AS DATE) - CAST(CAST('1992-01-01' AS TIMESTAMP) AS DATE) AS dd_str_col,
  (
    EXTRACT(YEAR FROM CAST(o_orderdate AS DATE)) - EXTRACT(YEAR FROM TO_DATE('1995-10-10 00:00:00', 'YYYY-MM-DD HH24:MI:SS'))
  ) * 12 + (
    EXTRACT(MONTH FROM CAST(o_orderdate AS DATE)) - EXTRACT(MONTH FROM TO_DATE('1995-10-10 00:00:00', 'YYYY-MM-DD HH24:MI:SS'))
  ) AS dd_pd_col,
  EXTRACT(YEAR FROM TO_DATE('1992-01-01 12:30:45', 'YYYY-MM-DD HH24:MI:SS')) - EXTRACT(YEAR FROM CAST(o_orderdate AS DATE)) AS dd_col_dt,
  FLOOR(
    (
      TO_DATE('1992-01-01 12:30:45', 'YYYY-MM-DD HH24:MI:SS') - CAST(CAST('1992-01-01' AS TIMESTAMP) AS DATE) + (
        MOD((
          TO_CHAR('1992-01-01', 'D') + -1
        ), 7)
      ) - (
        MOD(
          (
            TO_CHAR(TO_DATE('1992-01-01 12:30:45', 'YYYY-MM-DD HH24:MI:SS'), 'D') + -1
          ),
          7
        )
      )
    ) / 7
  ) AS dd_dt_str,
  MOD((
    TO_CHAR(o_orderdate, 'D') + -1
  ), 7) AS dow_col,
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
    WHEN TO_CHAR(o_orderdate, 'D') = 1
    THEN 'Sunday'
    WHEN TO_CHAR(o_orderdate, 'D') = 2
    THEN 'Monday'
    WHEN TO_CHAR(o_orderdate, 'D') = 3
    THEN 'Tuesday'
    WHEN TO_CHAR(o_orderdate, 'D') = 4
    THEN 'Wednesday'
    WHEN TO_CHAR(o_orderdate, 'D') = 5
    THEN 'Thursday'
    WHEN TO_CHAR(o_orderdate, 'D') = 6
    THEN 'Friday'
    WHEN TO_CHAR(o_orderdate, 'D') = 7
    THEN 'Saturday'
  END AS dayname_col,
  'Monday' AS dayname_str1,
  'Tuesday' AS dayname_str2,
  'Wednesday' AS dayname_str3,
  'Thursday' AS dayname_str4,
  'Friday' AS dayname_str5,
  'Saturday' AS dayname_str6,
  'Sunday' AS dayname_dt
FROM TPCH.ORDERS
