SELECT
  o_orderkey AS key,
  CONCAT_WS(
    '_',
    EXTRACT(YEAR FROM CAST(o_orderdate AS TIMESTAMP)),
    EXTRACT(QUARTER FROM CAST(o_orderdate AS TIMESTAMP)),
    EXTRACT(MONTH FROM CAST(o_orderdate AS TIMESTAMP)),
    EXTRACT(DAY FROM CAST(o_orderdate AS TIMESTAMP))
  ) AS a,
  CONCAT_WS(
    ':',
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
    END,
    (
      (
        DAYOFWEEK(TO_DATE(o_orderdate)) + -1
      ) % 7
    )
  ) AS b,
  DATEADD(DAY, -13, DATEADD(MONTH, 6, TRUNC(CAST(o_orderdate AS TIMESTAMP), 'YEAR'))) AS c,
  DATEADD(YEAR, 1, TRUNC(CAST(o_orderdate AS TIMESTAMP), 'QUARTER')) + INTERVAL '25' HOUR AS d,
  CAST('2025-01-01 12:35:00' AS TIMESTAMP) AS e,
  CAST('2025-07-22 12:00:00' AS TIMESTAMP) AS f,
  CAST('2025-01-01' AS DATE) AS g,
  CONCAT_WS(';', 12, 20, 6) AS h,
  YEAR(TO_DATE(o_orderdate)) - YEAR(TO_DATE(CAST('1993-05-25 12:45:36' AS TIMESTAMP))) AS i,
  (
    YEAR(TO_DATE(o_orderdate)) - YEAR(TO_DATE(CAST('1993-05-25 12:45:36' AS TIMESTAMP)))
  ) * 4 + QUARTER(o_orderdate) - QUARTER(CAST('1993-05-25 12:45:36' AS TIMESTAMP)) AS j,
  (
    YEAR(TO_DATE(o_orderdate)) - YEAR(TO_DATE(CAST('1993-05-25 12:45:36' AS TIMESTAMP)))
  ) * 12 + MONTH(TO_DATE(o_orderdate)) - MONTH(TO_DATE(CAST('1993-05-25 12:45:36' AS TIMESTAMP))) AS k,
  CAST(DATEDIFF(
    DAY,
    DATEADD(
      DAY,
      -(
        (
          DAYOFWEEK(TO_DATE(CAST('1993-05-25 12:45:36' AS TIMESTAMP))) + -1
        ) % 7
      ),
      CAST(CAST('1993-05-25 12:45:36' AS TIMESTAMP) AS DATE)
    ),
    DATEADD(
      DAY,
      -(
        (
          DAYOFWEEK(TO_DATE(o_orderdate)) + -1
        ) % 7
      ),
      CAST(o_orderdate AS DATE)
    )
  ) / 7 AS BIGINT) AS l,
  DATEDIFF(
    DAY,
    CAST(CAST('1993-05-25 12:45:36' AS TIMESTAMP) AS DATE),
    CAST(o_orderdate AS DATE)
  ) AS m,
  DATEDIFF(
    DAY,
    CAST(CAST('1993-05-25 12:45:36' AS TIMESTAMP) AS DATE),
    CAST(o_orderdate AS DATE)
  ) * 24 + EXTRACT(HOUR FROM CAST(o_orderdate AS TIMESTAMP)) - EXTRACT(HOUR FROM CAST('1993-05-25 12:45:36' AS TIMESTAMP)) AS n,
  (
    DATEDIFF(
      DAY,
      CAST(CAST('1993-05-25 12:45:36' AS TIMESTAMP) AS DATE),
      CAST(o_orderdate AS DATE)
    ) * 24 + EXTRACT(HOUR FROM CAST(o_orderdate AS TIMESTAMP)) - EXTRACT(HOUR FROM CAST('1993-05-25 12:45:36' AS TIMESTAMP))
  ) * 60 + EXTRACT(MINUTE FROM CAST(o_orderdate AS TIMESTAMP)) - EXTRACT(MINUTE FROM CAST('1993-05-25 12:45:36' AS TIMESTAMP)) AS o,
  (
    (
      DATEDIFF(
        DAY,
        CAST(CAST('1993-05-25 12:45:36' AS TIMESTAMP) AS DATE),
        CAST(o_orderdate AS DATE)
      ) * 24 + EXTRACT(HOUR FROM CAST(o_orderdate AS TIMESTAMP)) - EXTRACT(HOUR FROM CAST('1993-05-25 12:45:36' AS TIMESTAMP))
    ) * 60 + EXTRACT(MINUTE FROM CAST(o_orderdate AS TIMESTAMP)) - EXTRACT(MINUTE FROM CAST('1993-05-25 12:45:36' AS TIMESTAMP))
  ) * 60 + EXTRACT(SECOND FROM CAST(o_orderdate AS TIMESTAMP)) - EXTRACT(SECOND FROM CAST('1993-05-25 12:45:36' AS TIMESTAMP)) AS p,
  DATEADD(
    DAY,
    -(
      (
        DAYOFWEEK(TO_DATE(CAST(o_orderdate AS TIMESTAMP))) + -1
      ) % 7
    ),
    CAST(CAST(o_orderdate AS TIMESTAMP) AS DATE)
  ) AS q,
  CONCAT_WS(
    ':',
    MONTHNAME(o_orderdate),
    MONTHNAME(DATEADD(MONTH, 3, CAST(o_orderdate AS TIMESTAMP))),
    MONTHNAME(ADD_MONTHS(CAST(o_orderdate AS TIMESTAMP), -2))
  ) AS r
FROM tpch.orders
WHERE
  CONTAINS(o_comment, 'fo')
  AND ENDSWITH(o_clerk, '5')
  AND STARTSWITH(o_orderpriority, '3')
ORDER BY
  1
LIMIT 5
