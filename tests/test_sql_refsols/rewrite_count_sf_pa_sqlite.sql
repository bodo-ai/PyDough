SELECT
  COUNT(DISTINCT p_partkey) AS n
FROM tpch.part
WHERE
  p_brand = 'Brand#23'
