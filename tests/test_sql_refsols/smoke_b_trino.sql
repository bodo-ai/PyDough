SELECT
  o_orderkey AS key,
  CONCAT_WS(
    '_'[0],
    CAST(YEAR(CAST(o_orderdate AS TIMESTAMP))[0] AS VARCHAR),
    CAST(QUARTER(CAST(o_orderdate AS TIMESTAMP))[0] AS VARCHAR),
    CAST(MONTH(CAST(o_orderdate AS TIMESTAMP))[0] AS VARCHAR),
    CAST(DAY(CAST(o_orderdate AS TIMESTAMP))[0] AS VARCHAR)
  ) AS a,
  CONCAT_WS(
    ':'[0],
    (
      CASE
        WHEN (
          (
            DAY_OF_WEEK(o_orderdate) % 7
          ) + 1
        ) = 1
        THEN 'Monday'
        WHEN (
          (
            DAY_OF_WEEK(o_orderdate) % 7
          ) + 1
        ) = 2
        THEN 'Tuesday'
        WHEN (
          (
            DAY_OF_WEEK(o_orderdate) % 7
          ) + 1
        ) = 3
        THEN 'Wednesday'
        WHEN (
          (
            DAY_OF_WEEK(o_orderdate) % 7
          ) + 1
        ) = 4
        THEN 'Thursday'
        WHEN (
          (
            DAY_OF_WEEK(o_orderdate) % 7
          ) + 1
        ) = 5
        THEN 'Friday'
        WHEN (
          (
            DAY_OF_WEEK(o_orderdate) % 7
          ) + 1
        ) = 6
        THEN 'Saturday'
        WHEN (
          (
            DAY_OF_WEEK(o_orderdate) % 7
          ) + 1
        ) = 7
        THEN 'Sunday'
      END
    )[0],
    CAST((
      (
        DAY_OF_WEEK(o_orderdate) % 7
      ) + 1
    )[0] AS VARCHAR)
  ) AS b,
  DATE_ADD('DAY', -13, DATE_ADD('MONTH', 6, DATE_TRUNC('YEAR', CAST(o_orderdate AS TIMESTAMP)))) AS c,
  DATE_ADD(
    'HOUR',
    25,
    DATE_ADD('YEAR', 1, DATE_TRUNC('QUARTER', CAST(o_orderdate AS TIMESTAMP)))
  ) AS d,
  CAST('2025-01-01 12:35:00' AS TIMESTAMP) AS e,
  CAST('2025-07-22 12:00:00' AS TIMESTAMP) AS f,
  CAST('2025-01-01' AS DATE) AS g,
  CONCAT_WS(';'[0], CAST(12[0] AS VARCHAR), CAST(20[0] AS VARCHAR), CAST(6[0] AS VARCHAR)) AS h,
  DATE_DIFF('YEAR', CAST('1993-05-25 12:45:36' AS TIMESTAMP), CAST(o_orderdate AS TIMESTAMP)) AS i,
  DATE_DIFF('QUARTER', CAST('1993-05-25 12:45:36' AS TIMESTAMP), CAST(o_orderdate AS TIMESTAMP)) AS j,
  DATE_DIFF('MONTH', CAST('1993-05-25 12:45:36' AS TIMESTAMP), CAST(o_orderdate AS TIMESTAMP)) AS k,
  DATE_DIFF('WEEK', CAST('1993-05-25 12:45:36' AS TIMESTAMP), CAST(o_orderdate AS TIMESTAMP)) AS l,
  DATE_DIFF('DAY', CAST('1993-05-25 12:45:36' AS TIMESTAMP), CAST(o_orderdate AS TIMESTAMP)) AS m,
  DATE_DIFF('HOUR', CAST('1993-05-25 12:45:36' AS TIMESTAMP), CAST(o_orderdate AS TIMESTAMP)) AS n,
  DATE_DIFF('MINUTE', CAST('1993-05-25 12:45:36' AS TIMESTAMP), CAST(o_orderdate AS TIMESTAMP)) AS o,
  DATE_DIFF('SECOND', CAST('1993-05-25 12:45:36' AS TIMESTAMP), CAST(o_orderdate AS TIMESTAMP)) AS p,
  DATE_TRUNC(
    'DAY',
    DATE_ADD(
      'DAY',
      CAST((
        (
          DAY_OF_WEEK(CAST(o_orderdate AS TIMESTAMP)) % 7
        ) + 1
      ) AS BIGINT) * -1,
      CAST(o_orderdate AS TIMESTAMP)
    )
  ) AS q
FROM tpch.orders
WHERE
  STARTS_WITH(o_orderpriority, '3') AND o_clerk LIKE '%5' AND o_comment LIKE '%fo%'
ORDER BY
  1 NULLS FIRST
LIMIT 5
