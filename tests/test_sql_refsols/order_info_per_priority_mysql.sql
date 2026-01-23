WITH _t AS (
  SELECT
    o_orderkey,
    o_orderpriority,
    o_totalprice,
    ROW_NUMBER() OVER (PARTITION BY o_orderpriority ORDER BY CASE WHEN o_totalprice IS NULL THEN 1 ELSE 0 END DESC, o_totalprice DESC) AS _w
  FROM tpch.ORDERS
  WHERE
    EXTRACT(YEAR FROM CAST(o_orderdate AS DATETIME)) = 1992
)
SELECT
  o_orderpriority COLLATE utf8mb4_bin AS order_priority,
  o_orderkey AS order_key,
  o_totalprice AS order_total_price
FROM _t
WHERE
  _w = 1
ORDER BY
  1
