SELECT
  COUNT(DISTINCT p_partkey) AS n
FROM tpch.PART
WHERE
  p_brand = 'Brand#23'
