WITH _t AS (
  SELECT
    o_orderkey AS key,
    o_orderpriority AS order_priority,
    o_totalprice AS total_price,
    ROW_NUMBER() OVER (PARTITION BY o_orderpriority ORDER BY o_totalprice DESC) AS _w
  FROM tpch.orders
  WHERE
    CAST(STRFTIME('%Y', o_orderdate) AS INTEGER) = 1992
)
SELECT
  order_priority,
  key AS order_key,
  total_price AS order_total_price
FROM _t
WHERE
  _w = 1
ORDER BY
  order_priority
