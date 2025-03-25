SELECT
  order_date,
  DENSE_RANK() OVER (ORDER BY order_date NULLS LAST) AS rank
FROM (
  SELECT
    o_orderdate AS order_date
  FROM tpch.ORDERS
) AS _t0
