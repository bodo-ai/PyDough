WITH _s3 AS (
  SELECT
    partsupp.ps_suppkey,
    AVG(part.p_retailprice) AS avg_p_retailprice
  FROM tpch.partsupp AS partsupp
  JOIN tpch.part AS part
    ON part.p_partkey = partsupp.ps_partkey
  GROUP BY
    1
)
SELECT
  COUNT(DISTINCT supplier.s_suppkey) AS n
FROM tpch.supplier AS supplier
JOIN _s3 AS _s3
  ON _s3.ps_suppkey = supplier.s_suppkey
JOIN tpch.partsupp AS partsupp
  ON partsupp.ps_suppkey = supplier.s_suppkey
JOIN tpch.part AS part
  ON _s3.avg_p_retailprice > part.p_retailprice
  AND part.p_container = 'LG DRUM'
  AND part.p_partkey = partsupp.ps_partkey
  AND part.p_retailprice < (
    partsupp.ps_supplycost * 1.5
  )
WHERE
  supplier.s_acctbal < 1000 AND supplier.s_nationkey = 19
