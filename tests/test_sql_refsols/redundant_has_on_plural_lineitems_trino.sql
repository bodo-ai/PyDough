SELECT
  COUNT(DISTINCT l_orderkey) AS n
FROM postgres.tpch.lineitem
WHERE
  l_quantity > 49
