SELECT
  o_orderkey AS `key`,
  CONCAT_WS(
    '_',
    EXTRACT(YEAR FROM CAST(o_orderdate AS DATETIME)),
    EXTRACT(QUARTER FROM CAST(o_orderdate AS DATETIME)),
    EXTRACT(MONTH FROM CAST(o_orderdate AS DATETIME)),
    EXTRACT(DAY FROM CAST(o_orderdate AS DATETIME))
  ) AS a,
  CONCAT_WS(':', DAYNAME(o_orderdate), (
    (
      DAYOFWEEK(o_orderdate) + -1
    ) % 7
  )) AS b,
  DATE_SUB(
    DATE_ADD(
      STR_TO_DATE(CONCAT(YEAR(CAST(o_orderdate AS DATETIME)), ' 1 1'), '%Y %c %e'),
      INTERVAL '6' MONTH
    ),
    INTERVAL '13' DAY
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
  DATE_ADD(
    '0000-01-01 00:00:00',
    INTERVAL (TIMESTAMPDIFF(MINUTE, '0000-01-01 00:00:00', CAST('2025-01-01 12:35:13' AS DATETIME))) MINUTE
  ) AS e,
  DATE_ADD(
    DATE_ADD(
      DATE_ADD(
        '0000-01-01 00:00:00',
        INTERVAL (TIMESTAMPDIFF(HOUR, '0000-01-01 00:00:00', CAST('2025-01-01 12:35:13' AS DATETIME))) HOUR
      ),
      INTERVAL '2' QUARTER
    ),
    INTERVAL '3' WEEK
  ) AS f,
  CAST(CAST('2025-01-01 12:35:13' AS DATETIME) AS DATE) AS g,
  CONCAT_WS(
    ';',
    HOUR('2025-01-01 12:35:13'),
    MINUTE(CAST('2025-01-01 13:20:13' AS DATETIME)),
    SECOND(CAST('2025-01-01 12:35:06' AS DATETIME))
  ) AS h,
  YEAR(o_orderdate) - YEAR(CAST('1993-05-25 12:45:36' AS DATETIME)) AS i,
  (
    YEAR(o_orderdate) - YEAR(CAST('1993-05-25 12:45:36' AS DATETIME))
  ) * 4 + (
    QUARTER(o_orderdate) - QUARTER(CAST('1993-05-25 12:45:36' AS DATETIME))
  ) AS j,
  (
    YEAR(o_orderdate) - YEAR(CAST('1993-05-25 12:45:36' AS DATETIME))
  ) * 12 + (
    MONTH(o_orderdate) - MONTH(CAST('1993-05-25 12:45:36' AS DATETIME))
  ) AS k,
  CAST((
    DATEDIFF(CAST(o_orderdate AS DATETIME), CAST('1993-05-25 12:45:36' AS DATETIME)) + (
      (
        DAYOFWEEK(CAST('1993-05-25 12:45:36' AS DATETIME)) + -1
      ) % 7
    ) - (
      (
        DAYOFWEEK(o_orderdate) + -1
      ) % 7
    )
  ) / 7 AS SIGNED) AS l,
  DATEDIFF(o_orderdate, CAST('1993-05-25 12:45:36' AS DATETIME)) AS m,
  TIMESTAMPDIFF(
    HOUR,
    DATE_FORMAT(CAST('1993-05-25 12:45:36' AS DATETIME), '%Y-%m-%d %H:00:00'),
    DATE_FORMAT(o_orderdate, '%Y-%m-%d %H:00:00')
  ) AS n,
  TIMESTAMPDIFF(
    MINUTE,
    DATE_FORMAT(CAST('1993-05-25 12:45:36' AS DATETIME), '%Y-%m-%d %H:%i:00'),
    DATE_FORMAT(o_orderdate, '%Y-%m-%d %H:%i:00')
  ) AS o,
  TIMESTAMPDIFF(SECOND, CAST('1993-05-25 12:45:36' AS DATETIME), o_orderdate) AS p,
  CAST(DATE_SUB(
    CAST(o_orderdate AS DATETIME),
    INTERVAL (
      (
        DAYOFWEEK(CAST(o_orderdate AS DATETIME)) + -1
      ) % 7
    ) DAY
  ) AS DATE) AS q
FROM tpch.ORDERS
WHERE
  o_clerk LIKE '%5' AND o_comment LIKE '%fo%' AND o_orderpriority LIKE '3%'
ORDER BY
  1
LIMIT 5
