SELECT
  COUNT(*) AS n
FROM tpch.PARTSUPP AS PARTSUPP
JOIN tpch.PART AS PART
  ON PART.p_brand = 'Brand#23' AND PART.p_partkey = PARTSUPP.ps_partkey
