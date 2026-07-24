SELECT
  o_orderkey AS key,
  CONCAT_WS(
    '_',
    EXTRACT(YEAR FROM CAST(o_orderdate AS DATETIME)),
    EXTRACT(QUARTER FROM CAST(o_orderdate AS DATETIME)),
    EXTRACT(MONTH FROM CAST(o_orderdate AS DATETIME)),
    EXTRACT(DAY FROM CAST(o_orderdate AS DATETIME))
  ) AS a,
  CONCAT_WS(
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
  DATE_SUB(DATE_ADD(DATE_TRUNC('YEAR', CAST(o_orderdate AS TIMESTAMP)), 6, 'MONTH'), 13, DAY) AS c,
  DATE_ADD(
    DATE_ADD(DATE_TRUNC('QUARTER', CAST(o_orderdate AS TIMESTAMP)), 1, 'YEAR'),
    25,
    'HOUR'
  ) AS d,
  CAST('2025-01-01 12:35:00' AS TIMESTAMP) AS e,
  CAST('2025-07-22 12:00:00' AS TIMESTAMP) AS f,
  CAST('2025-01-01' AS DATE) AS g,
  CONCAT_WS(';', 12, 20, 6) AS h,
  DATEDIFF(CAST(o_orderdate AS DATETIME), CAST('1993-05-25 12:45:36' AS TIMESTAMP), YEAR) AS i,
  DATEDIFF(CAST(o_orderdate AS DATETIME), CAST('1993-05-25 12:45:36' AS TIMESTAMP), QUARTER) AS j,
  DATEDIFF(CAST(o_orderdate AS DATETIME), CAST('1993-05-25 12:45:36' AS TIMESTAMP), MONTH) AS k,
  DATEDIFF(CAST(o_orderdate AS DATETIME), CAST('1993-05-25 12:45:36' AS TIMESTAMP), WEEK) AS l,
  DATEDIFF(CAST(o_orderdate AS DATETIME), CAST('1993-05-25 12:45:36' AS TIMESTAMP), DAY) AS m,
  DATEDIFF(CAST(o_orderdate AS DATETIME), CAST('1993-05-25 12:45:36' AS TIMESTAMP), HOUR) AS n,
  DATEDIFF(CAST(o_orderdate AS DATETIME), CAST('1993-05-25 12:45:36' AS TIMESTAMP), MINUTE) AS o,
  DATEDIFF(CAST(o_orderdate AS DATETIME), CAST('1993-05-25 12:45:36' AS TIMESTAMP), SECOND) AS p,
  DATE_TRUNC('WEEK', CAST(o_orderdate AS TIMESTAMP)) AS q,
  CONCAT_WS(
    ':',
    CASE
      WHEN EXTRACT(MONTH FROM CAST(o_orderdate AS DATETIME)) = 1
      THEN 'Jan'
      WHEN EXTRACT(MONTH FROM CAST(o_orderdate AS DATETIME)) = 2
      THEN 'Feb'
      WHEN EXTRACT(MONTH FROM CAST(o_orderdate AS DATETIME)) = 3
      THEN 'Mar'
      WHEN EXTRACT(MONTH FROM CAST(o_orderdate AS DATETIME)) = 4
      THEN 'Apr'
      WHEN EXTRACT(MONTH FROM CAST(o_orderdate AS DATETIME)) = 5
      THEN 'May'
      WHEN EXTRACT(MONTH FROM CAST(o_orderdate AS DATETIME)) = 6
      THEN 'Jun'
      WHEN EXTRACT(MONTH FROM CAST(o_orderdate AS DATETIME)) = 7
      THEN 'Jul'
      WHEN EXTRACT(MONTH FROM CAST(o_orderdate AS DATETIME)) = 8
      THEN 'Aug'
      WHEN EXTRACT(MONTH FROM CAST(o_orderdate AS DATETIME)) = 9
      THEN 'Sep'
      WHEN EXTRACT(MONTH FROM CAST(o_orderdate AS DATETIME)) = 10
      THEN 'Oct'
      WHEN EXTRACT(MONTH FROM CAST(o_orderdate AS DATETIME)) = 11
      THEN 'Nov'
      ELSE 'Dec'
    END,
    CASE
      WHEN EXTRACT(MONTH FROM DATE_ADD(CAST(o_orderdate AS TIMESTAMP), 3, 'MONTH')) = 1
      THEN 'Jan'
      WHEN EXTRACT(MONTH FROM DATE_ADD(CAST(o_orderdate AS TIMESTAMP), 3, 'MONTH')) = 2
      THEN 'Feb'
      WHEN EXTRACT(MONTH FROM DATE_ADD(CAST(o_orderdate AS TIMESTAMP), 3, 'MONTH')) = 3
      THEN 'Mar'
      WHEN EXTRACT(MONTH FROM DATE_ADD(CAST(o_orderdate AS TIMESTAMP), 3, 'MONTH')) = 4
      THEN 'Apr'
      WHEN EXTRACT(MONTH FROM DATE_ADD(CAST(o_orderdate AS TIMESTAMP), 3, 'MONTH')) = 5
      THEN 'May'
      WHEN EXTRACT(MONTH FROM DATE_ADD(CAST(o_orderdate AS TIMESTAMP), 3, 'MONTH')) = 6
      THEN 'Jun'
      WHEN EXTRACT(MONTH FROM DATE_ADD(CAST(o_orderdate AS TIMESTAMP), 3, 'MONTH')) = 7
      THEN 'Jul'
      WHEN EXTRACT(MONTH FROM DATE_ADD(CAST(o_orderdate AS TIMESTAMP), 3, 'MONTH')) = 8
      THEN 'Aug'
      WHEN EXTRACT(MONTH FROM DATE_ADD(CAST(o_orderdate AS TIMESTAMP), 3, 'MONTH')) = 9
      THEN 'Sep'
      WHEN EXTRACT(MONTH FROM DATE_ADD(CAST(o_orderdate AS TIMESTAMP), 3, 'MONTH')) = 10
      THEN 'Oct'
      WHEN EXTRACT(MONTH FROM DATE_ADD(CAST(o_orderdate AS TIMESTAMP), 3, 'MONTH')) = 11
      THEN 'Nov'
      ELSE 'Dec'
    END,
    CASE
      WHEN EXTRACT(MONTH FROM DATE_SUB(CAST(o_orderdate AS TIMESTAMP), 2, MONTH)) = 1
      THEN 'Jan'
      WHEN EXTRACT(MONTH FROM DATE_SUB(CAST(o_orderdate AS TIMESTAMP), 2, MONTH)) = 2
      THEN 'Feb'
      WHEN EXTRACT(MONTH FROM DATE_SUB(CAST(o_orderdate AS TIMESTAMP), 2, MONTH)) = 3
      THEN 'Mar'
      WHEN EXTRACT(MONTH FROM DATE_SUB(CAST(o_orderdate AS TIMESTAMP), 2, MONTH)) = 4
      THEN 'Apr'
      WHEN EXTRACT(MONTH FROM DATE_SUB(CAST(o_orderdate AS TIMESTAMP), 2, MONTH)) = 5
      THEN 'May'
      WHEN EXTRACT(MONTH FROM DATE_SUB(CAST(o_orderdate AS TIMESTAMP), 2, MONTH)) = 6
      THEN 'Jun'
      WHEN EXTRACT(MONTH FROM DATE_SUB(CAST(o_orderdate AS TIMESTAMP), 2, MONTH)) = 7
      THEN 'Jul'
      WHEN EXTRACT(MONTH FROM DATE_SUB(CAST(o_orderdate AS TIMESTAMP), 2, MONTH)) = 8
      THEN 'Aug'
      WHEN EXTRACT(MONTH FROM DATE_SUB(CAST(o_orderdate AS TIMESTAMP), 2, MONTH)) = 9
      THEN 'Sep'
      WHEN EXTRACT(MONTH FROM DATE_SUB(CAST(o_orderdate AS TIMESTAMP), 2, MONTH)) = 10
      THEN 'Oct'
      WHEN EXTRACT(MONTH FROM DATE_SUB(CAST(o_orderdate AS TIMESTAMP), 2, MONTH)) = 11
      THEN 'Nov'
      ELSE 'Dec'
    END
  ) AS r
FROM tpch.orders
WHERE
  o_clerk LIKE '%5' AND o_comment LIKE '%fo%' AND o_orderpriority LIKE '3%'
ORDER BY
  1
LIMIT 5
