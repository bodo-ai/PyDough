WITH _s3 AS (
  SELECT DISTINCT
    partsupp.ps_suppkey
  FROM tpch.partsupp AS partsupp
  JOIN tpch.part AS part
    ON part.p_container LIKE 'SM%'
    AND part.p_partkey = partsupp.ps_partkey
    AND part.p_retailprice < (
      partsupp.ps_supplycost * 1.5
    )
)
SELECT
  COUNT(*) AS n
FROM tpch.supplier AS supplier
JOIN _s3 AS _s3
  ON _s3.ps_suppkey = supplier.s_suppkey
WHERE
  supplier.s_nationkey <= 3
