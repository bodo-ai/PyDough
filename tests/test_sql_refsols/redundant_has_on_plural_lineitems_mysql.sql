SELECT
  COUNT(DISTINCT l_orderkey) AS n
FROM tpch.LINEITEM
WHERE
  l_quantity > 49
