WITH _T AS (
  SELECT
    RANK() OVER (ORDER BY o_orderdate) AS _W
  FROM TPCH.ORDERS
)
SELECT
  COUNT(*) AS n_orders
FROM _T
WHERE
  _W = 1
