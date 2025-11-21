SELECT
  COUNT(DISTINCT partsupp.ps_suppkey) AS n
FROM tpch.supplier AS supplier
JOIN tpch.partsupp AS partsupp
  ON partsupp.ps_suppkey = supplier.s_suppkey
JOIN tpch.part AS part
  ON part.p_container LIKE 'SM%'
  AND part.p_partkey = partsupp.ps_partkey
  AND part.p_retailprice < (
    partsupp.ps_supplycost * 1.5
  )
WHERE
  supplier.s_nationkey <= 3
