SELECT
  o_orderkey AS key,
  LTRIM(
    NVL2(
      EXTRACT(YEAR FROM CAST(o_orderdate AS DATE)),
      '_' || EXTRACT(YEAR FROM CAST(o_orderdate AS DATE)),
      NULL
    ) || NVL2(
      EXTRACT(QUARTER FROM CAST(o_orderdate AS DATE)),
      '_' || EXTRACT(QUARTER FROM CAST(o_orderdate AS DATE)),
      NULL
    ) || NVL2(
      EXTRACT(MONTH FROM CAST(o_orderdate AS DATE)),
      '_' || EXTRACT(MONTH FROM CAST(o_orderdate AS DATE)),
      NULL
    ) || NVL2(
      EXTRACT(DAY FROM CAST(o_orderdate AS DATE)),
      '_' || EXTRACT(DAY FROM CAST(o_orderdate AS DATE)),
      NULL
    ),
    '_'
  ) AS a,
  LTRIM(
    NVL2(
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
      END,
      ':' || CASE
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
      END,
      NULL
    ) || NVL2(
      (
        MOD((
          TO_CHAR(o_orderdate, 'D') + -1
        ), 7)
      ),
      ':' || MOD((
        TO_CHAR(o_orderdate, 'D') + -1
      ), 7),
      NULL
    ),
    ':'
  ) AS b,
  TRUNC(CAST(o_orderdate AS TIMESTAMP), 'YEAR') + NUMTOYMINTERVAL(6, 'month') + NUMTODSINTERVAL(13, 'day') AS c,
  TRUNC(CAST(o_orderdate AS TIMESTAMP), 'QUARTER') + NUMTOYMINTERVAL(1, 'year') + NUMTODSINTERVAL(25, 'hour') AS d,
  TO_DATE('2025-01-01 12:35:00', 'YYYY-MM-DD HH24:MI:SS') AS e,
  TO_DATE('2025-07-22 12:00:00', 'YYYY-MM-DD HH24:MI:SS') AS f,
  TO_DATE('2025-01-01', 'YYYY-MM-DD') AS g,
  LTRIM(
    NVL2(12, ';' || 12, NULL) || NVL2(20, ';' || 20, NULL) || NVL2(6, ';' || 6, NULL),
    ';'
  ) AS h,
  EXTRACT(YEAR FROM CAST(o_orderdate AS DATE)) - EXTRACT(YEAR FROM CAST(CAST('1993-05-25 12:45:36' AS TIMESTAMP) AS DATE)) AS i,
  (
    EXTRACT(YEAR FROM CAST(o_orderdate AS DATE)) - EXTRACT(YEAR FROM CAST(CAST('1993-05-25 12:45:36' AS TIMESTAMP) AS DATE))
  ) * 4 + (
    EXTRACT(QUARTER FROM CAST(o_orderdate AS DATE)) - EXTRACT(QUARTER FROM CAST(CAST('1993-05-25 12:45:36' AS TIMESTAMP) AS DATE))
  ) AS j,
  (
    EXTRACT(YEAR FROM CAST(o_orderdate AS DATE)) - EXTRACT(YEAR FROM CAST(CAST('1993-05-25 12:45:36' AS TIMESTAMP) AS DATE))
  ) * 12 + (
    EXTRACT(MONTH FROM CAST(o_orderdate AS DATE)) - EXTRACT(MONTH FROM CAST(CAST('1993-05-25 12:45:36' AS TIMESTAMP) AS DATE))
  ) AS k,
  FLOOR(
    (
      CAST(o_orderdate AS DATE) - CAST(CAST('1993-05-25 12:45:36' AS TIMESTAMP) AS DATE) + (
        MOD((
          TO_CHAR('1993-05-25 12:45:36', 'D') + -1
        ), 7)
      ) - (
        MOD((
          TO_CHAR(o_orderdate, 'D') + -1
        ), 7)
      )
    ) / 7
  ) AS l,
  CAST(o_orderdate AS DATE) - CAST(CAST('1993-05-25 12:45:36' AS TIMESTAMP) AS DATE) AS m,
  (
    TRUNC(CAST(CAST(o_orderdate AS DATE) AS DATETIME), 'HH24') - TRUNC(CAST(CAST(CAST('1993-05-25 12:45:36' AS TIMESTAMP) AS DATE) AS DATETIME), 'HH24')
  ) * 24 AS n,
  (
    TRUNC(CAST(CAST(o_orderdate AS DATE) AS DATETIME), 'MI') - TRUNC(CAST(CAST(CAST('1993-05-25 12:45:36' AS TIMESTAMP) AS DATE) AS DATETIME), 'MI')
  ) * 1440 AS o,
  (
    CAST(o_orderdate AS DATE) - CAST(CAST('1993-05-25 12:45:36' AS TIMESTAMP) AS DATE)
  ) * 86400 AS p,
  TRUNC(CAST(o_orderdate AS TIMESTAMP), 'WEEK') AS q
FROM TPCH.ORDERS
WHERE
  o_clerk LIKE '%5' AND o_comment LIKE '%fo%' AND o_orderpriority LIKE '3%'
ORDER BY
  1 NULLS FIRST
FETCH FIRST 5 ROWS ONLY
