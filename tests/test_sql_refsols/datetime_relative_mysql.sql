WITH _t0 AS (
  SELECT
    o_orderdate
  FROM tpch.ORDERS
  ORDER BY
    o_custkey,
    1
  LIMIT 10
)
SELECT
  STR_TO_DATE(CONCAT(YEAR(CAST(o_orderdate AS DATETIME)), ' 1 1'), '%Y %c %e') AS d1,
  STR_TO_DATE(
    CONCAT(YEAR(CAST(o_orderdate AS DATETIME)), ' ', MONTH(CAST(o_orderdate AS DATETIME)), ' 1'),
    '%Y %c %e'
  ) AS d2,
  DATE_ADD(
    DATE_SUB(
      DATE_ADD(
        DATE_SUB(
          DATE_ADD(DATE_SUB(CAST(o_orderdate AS DATETIME), INTERVAL '11' YEAR), INTERVAL '9' MONTH),
          INTERVAL '7' DAY
        ),
        INTERVAL '5' HOUR
      ),
      INTERVAL '3' MINUTE
    ),
    INTERVAL '1' SECOND
  ) AS d3,
  CAST('2025-07-04 12:00:00' AS DATETIME) AS d4,
  CAST('2025-07-04 12:58:00' AS DATETIME) AS d5,
  CAST('2025-07-26 02:45:25' AS DATETIME) AS d6
FROM _t0
ORDER BY
  o_orderdate
