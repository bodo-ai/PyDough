SELECT
  COUNT(*) AS n
FROM tpch.partsupp AS partsupp
JOIN tpch.part AS part
  ON part.p_brand = 'Brand#23' AND part.p_partkey = partsupp.ps_partkey
