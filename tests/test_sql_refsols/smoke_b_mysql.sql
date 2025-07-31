SELECT
  o_orderkey AS `key`,
  CONCAT_WS('_', YEAR(o_orderdate), QUARTER(o_orderdate), MONTH(o_orderdate), DAY(o_orderdate)) AS a,
  CONCAT_WS(':', DAYNAME(o_orderdate), DAYOFWEEK(o_orderdate)) AS b,
  DATE_ADD(
    DATE_ADD(
      STR_TO_DATE(CONCAT(YEAR(CAST(o_orderdate AS DATETIME)), ' 1 1'), '%Y %c %e'),
      INTERVAL '6' MONTH
    ),
    INTERVAL '-13' DAY
  ) AS c,
  DATE_ADD(
    CAST(DATE_ADD(
      STR_TO_DATE(
        CONCAT(
          YEAR(CAST(o_orderdate AS DATETIME)),
          ' ',
          QUARTER(CAST(o_orderdate AS DATETIME)) * 3 - 2,
          ' 1'
        ),
        '%Y %c %e'
      ),
      INTERVAL '1' YEAR
    ) AS DATETIME),
    INTERVAL '25' HOUR
  ) AS d,
  DATE(CAST('2025-01-01 12:35:13' AS DATETIME)) AS e,
  CAST('2025-07-22' AS DATE) AS f,
  DATE(CAST('2025-01-01 12:35:13' AS DATETIME)) AS g,
  CONCAT_WS(
    ';',
    HOUR('2025-01-01 12:35:13'),
    MINUTE(CAST('2025-01-01 13:20:13' AS DATETIME)),
    SECOND(CAST('2025-01-01 12:35:06' AS DATETIME))
  ) AS h,
  DATEDIFF(CAST(o_orderdate AS DATETIME), CAST('1993-05-25 12:45:36' AS DATETIME)) AS i,
  DATEDIFF(CAST(o_orderdate AS DATETIME), CAST('1993-05-25 12:45:36' AS DATETIME)) AS j,
  DATEDIFF(CAST(o_orderdate AS DATETIME), CAST('1993-05-25 12:45:36' AS DATETIME)) AS k,
  DATEDIFF(CAST(o_orderdate AS DATETIME), CAST('1993-05-25 12:45:36' AS DATETIME)) AS l,
  DATEDIFF(CAST(o_orderdate AS DATETIME), CAST('1993-05-25 12:45:36' AS DATETIME)) AS m,
  DATEDIFF(CAST(o_orderdate AS DATETIME), CAST('1993-05-25 12:45:36' AS DATETIME)) AS n,
  DATEDIFF(CAST(o_orderdate AS DATETIME), CAST('1993-05-25 12:45:36' AS DATETIME)) AS o,
  DATEDIFF(CAST(o_orderdate AS DATETIME), CAST('1993-05-25 12:45:36' AS DATETIME)) AS p,
  STR_TO_DATE(
    CONCAT(YEAR(CAST(o_orderdate AS DATETIME)), ' ', WEEK(CAST(o_orderdate AS DATETIME), 1), ' 1'),
    '%Y %u %w'
  ) AS q
FROM tpch.ORDERS
WHERE
  o_clerk LIKE '%5' AND o_comment LIKE '%fo%' AND o_orderpriority LIKE '3%'
ORDER BY
  o_orderkey
LIMIT 5
