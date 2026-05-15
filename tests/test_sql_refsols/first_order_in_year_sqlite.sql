WITH _t AS (
  SELECT
    o_orderdate,
    o_orderkey,
    LAG(o_orderdate, 1) OVER (ORDER BY o_orderdate, o_orderkey) AS _w,
    LAG(o_orderdate, 1) OVER (ORDER BY o_orderdate, o_orderkey) AS _w_2
  FROM tpch.orders
  WHERE
    CAST(STRFTIME('%m', o_orderdate) AS INTEGER) = 1
)
SELECT
  o_orderdate AS order_date,
  o_orderkey AS key
FROM _t
WHERE
  CAST(STRFTIME('%Y', _w_2) AS INTEGER) <> CAST(STRFTIME('%Y', o_orderdate) AS INTEGER)
  OR _w IS NULL
ORDER BY
  1
