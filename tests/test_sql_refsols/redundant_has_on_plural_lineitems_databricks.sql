SELECT
  COUNT(DISTINCT l_orderkey) AS n
FROM tpch.lineitem
WHERE
  l_quantity > 49
