WITH _t AS (
  SELECT
    RANK() OVER (ORDER BY CASE WHEN o_orderdate IS NULL THEN 1 ELSE 0 END, o_orderdate) AS _w
  FROM tpch.ORDERS
)
SELECT
  COUNT(*) AS n_orders
FROM _t
WHERE
  _w = 1
