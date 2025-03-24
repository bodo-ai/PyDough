SELECT
  order_date,
  key AS o_orderkey,
  total_price AS o_totalprice
FROM (
  SELECT
    o_orderdate AS order_date,
    o_orderkey AS key,
    o_totalprice AS total_price
  FROM tpch.ORDERS
)
WHERE
  total_price < 1000.0
