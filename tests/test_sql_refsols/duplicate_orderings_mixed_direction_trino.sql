SELECT
  o_orderkey AS oid,
  o_totalprice AS amount,
  ROW_NUMBER() OVER (ORDER BY o_totalprice DESC NULLS FIRST, o_orderkey) AS r
FROM mysql.tpch.orders
WHERE
  o_custkey <= 2
ORDER BY
  2 DESC,
  1 NULLS FIRST
LIMIT 5
