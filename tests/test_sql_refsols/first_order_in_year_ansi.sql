WITH _t0 AS (
  SELECT
    o_orderkey AS key,
    o_orderdate AS order_date
  FROM tpch.orders
  QUALIFY
    EXTRACT(YEAR FROM LAG(o_orderdate, 1) OVER (ORDER BY o_orderdate NULLS LAST, o_orderkey NULLS LAST)) <> EXTRACT(YEAR FROM o_orderdate)
    OR LAG(o_orderdate, 1) OVER (ORDER BY o_orderdate NULLS LAST, o_orderkey NULLS LAST) IS NULL
)
SELECT
  order_date,
  key
FROM _t0
ORDER BY
  order_date
