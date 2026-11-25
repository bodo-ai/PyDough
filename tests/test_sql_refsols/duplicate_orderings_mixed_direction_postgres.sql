SELECT
  o_orderkey AS oid,
  o_totalprice AS amount,
  ROW_NUMBER() OVER (ORDER BY o_totalprice DESC, o_orderkey) AS r
FROM tpch.orders
WHERE
  o_custkey <= 2
ORDER BY
  2 DESC NULLS LAST,
  1 NULLS FIRST
LIMIT 5
