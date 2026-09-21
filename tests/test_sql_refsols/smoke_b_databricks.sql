SELECT
  o_orderkey AS key,
  CASE
    WHEN EXTRACT(DAY FROM CAST(o_orderdate AS TIMESTAMP)) IS NULL
    OR EXTRACT(MONTH FROM CAST(o_orderdate AS TIMESTAMP)) IS NULL
    OR EXTRACT(QUARTER FROM CAST(o_orderdate AS TIMESTAMP)) IS NULL
    OR EXTRACT(YEAR FROM CAST(o_orderdate AS TIMESTAMP)) IS NULL
    THEN NULL
    ELSE CONCAT_WS(
      '_',
      EXTRACT(YEAR FROM CAST(o_orderdate AS TIMESTAMP)),
      EXTRACT(QUARTER FROM CAST(o_orderdate AS TIMESTAMP)),
      EXTRACT(MONTH FROM CAST(o_orderdate AS TIMESTAMP)),
      EXTRACT(DAY FROM CAST(o_orderdate AS TIMESTAMP))
    )
  END AS a,
  CASE
    WHEN (
      (
        DAYOFWEEK(o_orderdate) + -1
      ) % 7
    ) IS NULL
    OR CASE
      WHEN DAYOFWEEK(o_orderdate) = 1
      THEN 'Sunday'
      WHEN DAYOFWEEK(o_orderdate) = 2
      THEN 'Monday'
      WHEN DAYOFWEEK(o_orderdate) = 3
      THEN 'Tuesday'
      WHEN DAYOFWEEK(o_orderdate) = 4
      THEN 'Wednesday'
      WHEN DAYOFWEEK(o_orderdate) = 5
      THEN 'Thursday'
      WHEN DAYOFWEEK(o_orderdate) = 6
      THEN 'Friday'
      WHEN DAYOFWEEK(o_orderdate) = 7
      THEN 'Saturday'
    END IS NULL
    THEN NULL
    ELSE CONCAT_WS(
      ':',
      CASE
        WHEN DAYOFWEEK(o_orderdate) = 1
        THEN 'Sunday'
        WHEN DAYOFWEEK(o_orderdate) = 2
        THEN 'Monday'
        WHEN DAYOFWEEK(o_orderdate) = 3
        THEN 'Tuesday'
        WHEN DAYOFWEEK(o_orderdate) = 4
        THEN 'Wednesday'
        WHEN DAYOFWEEK(o_orderdate) = 5
        THEN 'Thursday'
        WHEN DAYOFWEEK(o_orderdate) = 6
        THEN 'Friday'
        WHEN DAYOFWEEK(o_orderdate) = 7
        THEN 'Saturday'
      END,
      (
        (
          DAYOFWEEK(o_orderdate) + -1
        ) % 7
      )
    )
  END AS b,
  DATE_ADD(DATE_ADD(MONTH, 6, TRUNC(CAST(o_orderdate AS TIMESTAMP), 'YEAR')), -13) AS c,
  DATE_ADD(YEAR, 1, TRUNC(CAST(o_orderdate AS TIMESTAMP), 'QUARTER')) + INTERVAL '25' HOUR AS d,
  CAST('2025-01-01 12:35:00' AS TIMESTAMP) AS e,
  CAST('2025-07-22 12:00:00' AS TIMESTAMP) AS f,
  CAST('2025-01-01' AS DATE) AS g,
  CONCAT_WS(';', 12, 20, 6) AS h,
  YEAR(o_orderdate) - YEAR(CAST('1993-05-25 12:45:36' AS TIMESTAMP)) AS i,
  (
    YEAR(o_orderdate) - YEAR(CAST('1993-05-25 12:45:36' AS TIMESTAMP))
  ) * 4 + QUARTER(o_orderdate) - QUARTER(CAST('1993-05-25 12:45:36' AS TIMESTAMP)) AS j,
  (
    YEAR(o_orderdate) - YEAR(CAST('1993-05-25 12:45:36' AS TIMESTAMP))
  ) * 12 + MONTH(o_orderdate) - MONTH(CAST('1993-05-25 12:45:36' AS TIMESTAMP)) AS k,
  CAST(DATEDIFF(
    DAY,
    DATE_ADD(
      CAST(CAST('1993-05-25 12:45:36' AS TIMESTAMP) AS DATE),
      -(
        (
          DAYOFWEEK(CAST('1993-05-25 12:45:36' AS TIMESTAMP)) + -1
        ) % 7
      )
    ),
    DATE_ADD(CAST(o_orderdate AS DATE), -(
      (
        DAYOFWEEK(o_orderdate) + -1
      ) % 7
    ))
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
  DATE_ADD(
    CAST(CAST(o_orderdate AS TIMESTAMP) AS DATE),
    -(
      (
        DAYOFWEEK(CAST(o_orderdate AS TIMESTAMP)) + -1
      ) % 7
    )
  ) AS q,
  CASE
    WHEN MONTHNAME(ADD_MONTHS(CAST(o_orderdate AS TIMESTAMP), -2)) IS NULL
    OR MONTHNAME(DATE_ADD(MONTH, 3, CAST(o_orderdate AS TIMESTAMP))) IS NULL
    OR MONTHNAME(o_orderdate) IS NULL
    THEN NULL
    ELSE CONCAT_WS(
      ':',
      MONTHNAME(o_orderdate),
      MONTHNAME(DATE_ADD(MONTH, 3, CAST(o_orderdate AS TIMESTAMP))),
      MONTHNAME(ADD_MONTHS(CAST(o_orderdate AS TIMESTAMP), -2))
    )
  END AS r
FROM tpch.orders
WHERE
  CONTAINS(o_comment, 'fo')
  AND ENDSWITH(o_clerk, '5')
  AND STARTSWITH(o_orderpriority, '3')
ORDER BY
  1
LIMIT 5
