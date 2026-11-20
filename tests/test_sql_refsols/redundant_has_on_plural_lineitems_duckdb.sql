SELECT
  COUNT(DISTINCT l_orderkey) AS n
FROM main.lineitem
WHERE
  l_quantity > 49
