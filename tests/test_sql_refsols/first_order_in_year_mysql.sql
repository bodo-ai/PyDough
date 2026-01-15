WITH _t AS (
  SELECT
    o_orderdate,
    o_orderkey,
    LAG(o_orderdate, 1) OVER (ORDER BY CASE WHEN o_orderdate IS NULL THEN 1 ELSE 0 END, o_orderdate, CASE WHEN o_orderkey IS NULL THEN 1 ELSE 0 END, o_orderkey) AS _w,
    LAG(o_orderdate, 1) OVER (ORDER BY CASE WHEN o_orderdate IS NULL THEN 1 ELSE 0 END, o_orderdate, CASE WHEN o_orderkey IS NULL THEN 1 ELSE 0 END, o_orderkey) AS _w_2
  FROM tpch.ORDERS
  WHERE
    EXTRACT(MONTH FROM CAST(o_orderdate AS DATETIME)) = 1
)
SELECT
  o_orderdate AS order_date,
  o_orderkey AS `key`
FROM _t
WHERE
  EXTRACT(YEAR FROM CAST(_w_2 AS DATETIME)) <> EXTRACT(YEAR FROM CAST(o_orderdate AS DATETIME))
  OR _w IS NULL
ORDER BY
  1
