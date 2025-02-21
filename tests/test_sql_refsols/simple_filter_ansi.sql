SELECT
  order_date,
  o_orderkey,
  o_totalprice
FROM (
  SELECT
    o_orderdate AS order_date,
    o_orderkey AS o_orderkey,
    o_totalprice AS o_totalprice
  FROM tpch.ORDERS
)
WHERE
  o_totalprice < 1000.0
