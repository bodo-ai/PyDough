WITH _s0 AS (
  SELECT
    ps_partkey,
    ps_suppkey
  FROM tpch.partsupp
), _s3 AS (
  SELECT
    SUM(IIF(NOT part.p_retailprice IS NULL, 1, 0)) AS sum_expr_1,
    SUM(part.p_retailprice) AS sum_p_retailprice,
    _s0.ps_suppkey
  FROM _s0 AS _s0
  JOIN tpch.part AS part
    ON _s0.ps_partkey = part.p_partkey
  GROUP BY
    _s0.ps_suppkey
)
SELECT
  COUNT(DISTINCT supplier.s_suppkey) AS n
FROM tpch.supplier AS supplier
JOIN _s3 AS _s3
  ON _s3.ps_suppkey = supplier.s_suppkey
JOIN _s0 AS _s5
  ON _s5.ps_suppkey = supplier.s_suppkey
JOIN tpch.part AS part
  ON _s5.ps_partkey = part.p_partkey
  AND part.p_container = 'LG DRUM'
  AND part.p_retailprice < (
    CAST(_s3.sum_p_retailprice AS REAL) / _s3.sum_expr_1
  )
WHERE
  supplier.s_acctbal < 1000 AND supplier.s_nationkey = 19
