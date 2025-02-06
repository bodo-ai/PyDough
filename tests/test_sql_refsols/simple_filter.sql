SELECT
  o_orderkey,
  o_totalprice
FROM (
  SELECT
    o_orderkey AS o_orderkey,
    o_totalprice AS o_totalprice
  FROM tpch.ORDERS
)
WHERE
  o_totalprice < 1000.0
