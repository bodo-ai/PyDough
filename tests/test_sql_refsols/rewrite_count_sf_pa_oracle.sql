SELECT
  COUNT(DISTINCT p_partkey) AS n
FROM TPCH.PART
WHERE
  p_brand = 'Brand#23'
