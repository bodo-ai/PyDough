WITH _t1 AS (
  SELECT
    o_orderdate,
    o_orderkey
  FROM tpch.orders
  WHERE
    EXTRACT(MONTH FROM CAST(o_orderdate AS TIMESTAMP)) = 1
  QUALIFY
    EXTRACT(YEAR FROM CAST(LAG(o_orderdate, 1) OVER (ORDER BY o_orderdate, o_orderkey) AS TIMESTAMP)) <> EXTRACT(YEAR FROM CAST(o_orderdate AS TIMESTAMP))
    OR LAG(o_orderdate, 1) OVER (ORDER BY o_orderdate, o_orderkey) IS NULL
)
SELECT
  o_orderdate AS order_date,
  o_orderkey AS key
FROM _t1
ORDER BY
  1 NULLS FIRST
