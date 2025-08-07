WITH _s0 AS (
  SELECT
    AVG(p_retailprice) AS global_avg_price
  FROM tpch.part
), _s2 AS (
  SELECT
    ps_partkey,
    ps_suppkey
  FROM tpch.partsupp
), _s5 AS (
  SELECT
    SUM(CASE WHEN NOT part.p_retailprice IS NULL THEN 1 ELSE 0 END) AS sum_expr_1,
    SUM(part.p_retailprice) AS sum_p_retailprice,
    _s2.ps_suppkey
  FROM _s2 AS _s2
  JOIN tpch.part AS part
    ON _s2.ps_partkey = part.p_partkey
  GROUP BY
    _s2.ps_suppkey
)
SELECT
  COUNT(DISTINCT supplier.s_suppkey) AS n
FROM _s0 AS _s0
JOIN tpch.supplier AS supplier
  ON supplier.s_acctbal < 1000 AND supplier.s_nationkey = 19
JOIN _s5 AS _s5
  ON _s5.ps_suppkey = supplier.s_suppkey
JOIN _s2 AS _s7
  ON _s7.ps_suppkey = supplier.s_suppkey
JOIN tpch.part AS part
  ON _s7.ps_partkey = part.p_partkey
  AND part.p_container = 'LG DRUM'
  AND part.p_retailprice < (
    _s0.global_avg_price * 0.85
  )
  AND part.p_retailprice < (
    _s5.sum_p_retailprice / _s5.sum_expr_1
  )
