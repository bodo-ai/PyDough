SELECT
  COUNT(DISTINCT partsupp.ps_suppkey) AS n
FROM tpch.partsupp AS partsupp
JOIN tpch.part AS part
  ON part.p_container LIKE 'SM%'
  AND part.p_partkey = partsupp.ps_partkey
  AND part.p_retailprice < (
    partsupp.ps_supplycost * 1.5
  )
