WITH _s0 AS (
  SELECT
    AVG(p_retailprice) AS avg_p_retailprice
  FROM tpch.part
), _s5 AS (
  SELECT
    SUM(IIF(NOT part.p_retailprice IS NULL, 1, 0)) AS sum_expr,
    SUM(part.p_retailprice) AS sum_p_retailprice,
    partsupp.ps_suppkey
  FROM tpch.partsupp AS partsupp
  JOIN tpch.part AS part
    ON part.p_partkey = partsupp.ps_partkey
  GROUP BY
    3
)
SELECT
  COUNT(DISTINCT supplier.s_suppkey) AS n
FROM _s0 AS _s0
JOIN tpch.supplier AS supplier
  ON supplier.s_acctbal < 1000 AND supplier.s_nationkey = 19
JOIN _s5 AS _s5
  ON _s5.ps_suppkey = supplier.s_suppkey
JOIN tpch.partsupp AS partsupp
  ON partsupp.ps_suppkey = supplier.s_suppkey
JOIN tpch.part AS part
  ON part.p_container = 'LG DRUM'
  AND part.p_partkey = partsupp.ps_partkey
  AND part.p_retailprice < (
    CAST(_s5.sum_p_retailprice AS REAL) / _s5.sum_expr
  )
  AND part.p_retailprice < (
    _s0.avg_p_retailprice * 0.85
  )
  AND part.p_retailprice < (
    partsupp.ps_supplycost * 1.5
  )
