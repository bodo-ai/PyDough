WITH _t0 AS (
  SELECT
    CAST((
      JULIANDAY(DATE(o_orderdate, 'start of day')) - JULIANDAY(
        DATE(
          LAG(o_orderdate, 1) OVER (PARTITION BY o_clerk ORDER BY o_orderdate),
          'start of day'
        )
      )
    ) AS INTEGER) AS delta
  FROM tpch.orders
  WHERE
    o_orderpriority = '1-URGENT'
)
SELECT
  AVG(delta) AS avg_delta
FROM _t0
