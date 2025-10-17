WITH _s0 AS (
  SELECT
    AVG(p_retailprice) AS avg_p_retailprice
  FROM tpch.part
), _s6 AS (
  SELECT
    MAX(_s0.avg_p_retailprice) AS anything_avg_p_retailprice,
    MAX(supplier.s_suppkey) AS anything_s_suppkey,
    AVG(part.p_retailprice) AS supplier_avg_price
  FROM _s0 AS _s0
  JOIN tpch.supplier AS supplier
    ON supplier.s_acctbal < 1000 AND supplier.s_nationkey = 19
  JOIN tpch.partsupp AS partsupp
    ON partsupp.ps_suppkey = supplier.s_suppkey
  JOIN tpch.part AS part
    ON part.p_partkey = partsupp.ps_partkey
  GROUP BY
    partsupp.ps_suppkey
)
SELECT
  COUNT(DISTINCT _s6.anything_s_suppkey) AS n
FROM _s6 AS _s6
JOIN tpch.partsupp AS partsupp
  ON _s6.anything_s_suppkey = partsupp.ps_suppkey
JOIN tpch.part AS part
  ON _s6.supplier_avg_price > part.p_retailprice
  AND part.p_container = 'LG DRUM'
  AND part.p_partkey = partsupp.ps_partkey
  AND part.p_retailprice < (
    _s6.anything_avg_p_retailprice * 0.85
  )
  AND part.p_retailprice < (
    partsupp.ps_supplycost * 1.5
  )
