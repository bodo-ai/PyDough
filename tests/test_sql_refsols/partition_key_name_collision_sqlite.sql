SELECT
  o_custkey AS key,
  COALESCE(SUM(o_totalprice), 0) AS total
FROM tpch.orders
WHERE
  o_custkey <= 3
GROUP BY
  1
