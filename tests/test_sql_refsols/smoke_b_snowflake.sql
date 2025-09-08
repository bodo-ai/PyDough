SELECT
  o_orderkey AS key,
  CONCAT_WS(
    '_',
    YEAR(CAST(o_orderdate AS TIMESTAMP)),
    QUARTER(CAST(o_orderdate AS TIMESTAMP)),
    MONTH(CAST(o_orderdate AS TIMESTAMP)),
    DAY(CAST(o_orderdate AS TIMESTAMP))
  ) AS a,
  CONCAT_WS(
    ':',
    CASE
      WHEN DAYOFWEEK(o_orderdate) = 0
      THEN 'Sunday'
      WHEN DAYOFWEEK(o_orderdate) = 1
      THEN 'Monday'
      WHEN DAYOFWEEK(o_orderdate) = 2
      THEN 'Tuesday'
      WHEN DAYOFWEEK(o_orderdate) = 3
      THEN 'Wednesday'
      WHEN DAYOFWEEK(o_orderdate) = 4
      THEN 'Thursday'
      WHEN DAYOFWEEK(o_orderdate) = 5
      THEN 'Friday'
      WHEN DAYOFWEEK(o_orderdate) = 6
      THEN 'Saturday'
    END,
    DAYOFWEEK(o_orderdate)
  ) AS b,
  DATEADD(DAY, -13, DATEADD(MONTH, 6, DATE_TRUNC('YEAR', CAST(o_orderdate AS TIMESTAMP)))) AS c,
  DATEADD(HOUR, 25, DATEADD(YEAR, 1, DATE_TRUNC('QUARTER', CAST(o_orderdate AS TIMESTAMP)))) AS d,
  CAST('2025-01-01 12:35:00' AS TIMESTAMP) AS e,
  CAST('2025-07-22 12:00:00' AS TIMESTAMP) AS f,
  CAST('2025-01-01' AS DATE) AS g,
  CONCAT_WS(';', 12, 20, 6) AS h,
  DATEDIFF(YEAR, CAST('1993-05-25 12:45:36' AS TIMESTAMP), CAST(o_orderdate AS DATETIME)) AS i,
  DATEDIFF(QUARTER, CAST('1993-05-25 12:45:36' AS TIMESTAMP), CAST(o_orderdate AS DATETIME)) AS j,
  DATEDIFF(MONTH, CAST('1993-05-25 12:45:36' AS TIMESTAMP), CAST(o_orderdate AS DATETIME)) AS k,
  DATEDIFF(
    WEEK,
    DATEADD(
      DAY,
      DAYOFWEEK(CAST('1993-05-25 12:45:36' AS TIMESTAMP)) * -1,
      CAST('1993-05-25 12:45:36' AS TIMESTAMP)
    ),
    CAST(DATEADD(DAY, DAYOFWEEK(o_orderdate) * -1, o_orderdate) AS DATETIME)
  ) AS l,
  DATEDIFF(DAY, CAST('1993-05-25 12:45:36' AS TIMESTAMP), CAST(o_orderdate AS DATETIME)) AS m,
  DATEDIFF(HOUR, CAST('1993-05-25 12:45:36' AS TIMESTAMP), CAST(o_orderdate AS DATETIME)) AS n,
  DATEDIFF(MINUTE, CAST('1993-05-25 12:45:36' AS TIMESTAMP), CAST(o_orderdate AS DATETIME)) AS o,
  DATEDIFF(SECOND, CAST('1993-05-25 12:45:36' AS TIMESTAMP), CAST(o_orderdate AS DATETIME)) AS p,
  DATE_TRUNC(
    'DAY',
    DATEADD(DAY, DAYOFWEEK(CAST(o_orderdate AS TIMESTAMP)) * -1, CAST(o_orderdate AS TIMESTAMP))
  ) AS q
FROM tpch.orders
WHERE
  CONTAINS(o_comment, 'fo')
  AND ENDSWITH(o_clerk, '5')
  AND STARTSWITH(o_orderpriority, '3')
ORDER BY
  1 NULLS FIRST
LIMIT 5
