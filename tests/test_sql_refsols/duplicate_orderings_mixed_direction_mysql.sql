SELECT
  o_orderkey AS oid,
  o_totalprice AS amount,
  ROW_NUMBER() OVER (
    ORDER BY CASE WHEN o_totalprice IS NULL THEN 1 ELSE 0 END DESC, o_totalprice DESC, CASE WHEN o_orderkey IS NULL THEN 1 ELSE 0 END, o_orderkey
  ) AS r
FROM tpch.ORDERS
WHERE
  o_custkey <= 2
ORDER BY
  2 DESC,
  1
LIMIT 5
