SELECT
  COUNT(DISTINCT o_custkey) AS n
FROM tpch.ORDERS
WHERE
  o_totalprice > 400000
