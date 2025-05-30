WITH _t AS (
  SELECT
    o_orderkey AS key,
    o_orderdate AS order_date,
    LAG(o_orderdate, 1) OVER (ORDER BY o_orderdate, o_orderkey) AS _w,
    LAG(o_orderdate, 1) OVER (ORDER BY o_orderdate, o_orderkey) AS _w_2
  FROM tpch.orders
)
SELECT
  order_date,
  key
FROM _t
WHERE
  CAST(STRFTIME('%Y', _w_2) AS INTEGER) <> CAST(STRFTIME('%Y', order_date) AS INTEGER)
  OR _w IS NULL
ORDER BY
  order_date
