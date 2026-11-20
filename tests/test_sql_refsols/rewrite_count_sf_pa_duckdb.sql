SELECT
  COUNT(*) AS n
FROM main.partsupp AS partsupp
JOIN main.part AS part
  ON part.p_brand = 'Brand#23' AND part.p_partkey = partsupp.ps_partkey
