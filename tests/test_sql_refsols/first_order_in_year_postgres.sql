WITH _t AS (
  SELECT
    o_orderdate,
    o_orderkey,
    LAG(o_orderdate, 1) OVER (ORDER BY o_orderdate, o_orderkey) AS _w,
    LAG(o_orderdate, 1) OVER (ORDER BY o_orderdate, o_orderkey) AS _w_2
  FROM tpch.orders
  WHERE
    EXTRACT(MONTH FROM CAST(o_orderdate AS TIMESTAMP)) = 1
)
SELECT
  o_orderdate AS order_date,
  o_orderkey AS key
FROM _t
WHERE
  EXTRACT(YEAR FROM CAST(_w_2 AS TIMESTAMP)) <> EXTRACT(YEAR FROM CAST(o_orderdate AS TIMESTAMP))
  OR _w IS NULL
ORDER BY
  1 NULLS FIRST
