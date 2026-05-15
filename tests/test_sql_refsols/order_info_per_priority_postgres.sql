WITH _t AS (
  SELECT
    o_orderkey,
    o_orderpriority,
    o_totalprice,
    ROW_NUMBER() OVER (PARTITION BY o_orderpriority ORDER BY o_totalprice DESC) AS _w
  FROM tpch.orders
  WHERE
    EXTRACT(YEAR FROM CAST(o_orderdate AS TIMESTAMP)) = 1992
)
SELECT
  o_orderpriority AS order_priority,
  o_orderkey AS order_key,
  o_totalprice AS order_total_price
FROM _t
WHERE
  _w = 1
ORDER BY
  1 NULLS FIRST
