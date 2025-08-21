WITH _t0 AS (
  SELECT
    ROUND(CUME_DIST() OVER (ORDER BY o_orderpriority), 4) AS c
  FROM tpch.orders
  WHERE
    (
      CAST(STRFTIME('%Y', o_orderdate) AS INTEGER) = 1992
      OR CAST(STRFTIME('%Y', o_orderdate) AS INTEGER) = 1998
    )
    AND (
      CAST(STRFTIME('%Y', o_orderdate) AS INTEGER) = 1998 OR o_orderpriority = '2-HIGH'
    )
)
SELECT
  c,
  COUNT(*) AS n
FROM _t0
GROUP BY
  1
ORDER BY
  1
