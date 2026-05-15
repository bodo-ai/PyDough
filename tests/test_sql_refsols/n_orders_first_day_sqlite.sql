WITH _t AS (
  SELECT
    RANK() OVER (ORDER BY o_orderdate) AS _w
  FROM tpch.orders
)
SELECT
  COUNT(*) AS n_orders
FROM _t
WHERE
  _w = 1
