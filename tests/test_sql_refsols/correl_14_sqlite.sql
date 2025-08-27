WITH _s3 AS (
  SELECT
    AVG(part.p_retailprice) AS supplier_avg_price,
    partsupp.ps_suppkey
  FROM tpch.partsupp AS partsupp
  JOIN tpch.part AS part
    ON part.p_partkey = partsupp.ps_partkey
  GROUP BY
    2
)
SELECT
  COUNT(DISTINCT supplier.s_suppkey) AS n
FROM tpch.supplier AS supplier
JOIN _s3 AS _s3
  ON _s3.ps_suppkey = supplier.s_suppkey
JOIN tpch.partsupp AS partsupp
  ON partsupp.ps_suppkey = supplier.s_suppkey
JOIN tpch.part AS part
  ON _s3.supplier_avg_price > part.p_retailprice
  AND part.p_container = 'LG DRUM'
  AND part.p_partkey = partsupp.ps_partkey
  AND part.p_retailprice < (
    partsupp.ps_supplycost * 1.5
  )
WHERE
  supplier.s_acctbal < 1000 AND supplier.s_nationkey = 19
