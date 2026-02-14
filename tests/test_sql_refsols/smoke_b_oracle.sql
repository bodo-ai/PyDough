SELECT
  o_orderkey AS key,
  LISTAGG(
    '_',
    EXTRACT(YEAR FROM CAST(o_orderdate AS DATETIME)),
    EXTRACT(QUARTER FROM CAST(o_orderdate AS DATETIME)),
    EXTRACT(MONTH FROM CAST(o_orderdate AS DATETIME)),
    EXTRACT(DAY FROM CAST(o_orderdate AS DATETIME))
  ) AS a,
  LISTAGG(
    ':',
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
    END,
    DAY_OF_WEEK(o_orderdate)
  ) AS b,
  DATE_SUB(DATE_ADD(TRUNC(CAST(o_orderdate AS TIMESTAMP), 'YEAR'), 6, 'MONTH'), 13, DAY) AS c,
  DATE_ADD(DATE_ADD(TRUNC(CAST(o_orderdate AS TIMESTAMP), 'QUARTER'), 1, 'YEAR'), 25, 'HOUR') AS d,
  TIME_STR_TO_TIME('2025-01-01 12:35:00') AS e,
  TIME_STR_TO_TIME('2025-07-22 12:00:00') AS f,
  TO_DATE('2025-01-01', 'YYYY-MM-DD') AS g,
  LISTAGG(';', 12, 20, 6) AS h,
  DATEDIFF(CAST(o_orderdate AS DATETIME), CAST('1993-05-25 12:45:36' AS TIMESTAMP), YEAR) AS i,
  DATEDIFF(CAST(o_orderdate AS DATETIME), CAST('1993-05-25 12:45:36' AS TIMESTAMP), QUARTER) AS j,
  DATEDIFF(CAST(o_orderdate AS DATETIME), CAST('1993-05-25 12:45:36' AS TIMESTAMP), MONTH) AS k,
  DATEDIFF(CAST(o_orderdate AS DATETIME), CAST('1993-05-25 12:45:36' AS TIMESTAMP), WEEK) AS l,
  DATEDIFF(CAST(o_orderdate AS DATETIME), CAST('1993-05-25 12:45:36' AS TIMESTAMP), DAY) AS m,
  DATEDIFF(CAST(o_orderdate AS DATETIME), CAST('1993-05-25 12:45:36' AS TIMESTAMP), HOUR) AS n,
  DATEDIFF(CAST(o_orderdate AS DATETIME), CAST('1993-05-25 12:45:36' AS TIMESTAMP), MINUTE) AS o,
  DATEDIFF(CAST(o_orderdate AS DATETIME), CAST('1993-05-25 12:45:36' AS TIMESTAMP), SECOND) AS p,
  TRUNC(CAST(o_orderdate AS TIMESTAMP), 'WEEK') AS q
FROM TPCH.ORDERS
WHERE
  o_clerk LIKE '%5' AND o_comment LIKE '%fo%' AND o_orderpriority LIKE '3%'
ORDER BY
  1 NULLS FIRST
FETCH FIRST 5 ROWS ONLY
