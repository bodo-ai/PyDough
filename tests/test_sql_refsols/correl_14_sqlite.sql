WITH _s4 AS (
  SELECT
    partsupp.ps_suppkey,
    SUM(NOT part.p_retailprice IS NULL) AS sum_expr,
    SUM(part.p_retailprice) AS sum_p_retailprice
  FROM tpch.supplier AS supplier
  JOIN tpch.partsupp AS partsupp
    ON partsupp.ps_suppkey = supplier.s_suppkey
  JOIN tpch.part AS part
    ON part.p_partkey = partsupp.ps_partkey
  WHERE
    supplier.s_acctbal < 1000 AND supplier.s_nationkey = 19
  GROUP BY
    1
)
SELECT
  COUNT(DISTINCT _s4.ps_suppkey) AS n
FROM _s4 AS _s4
JOIN tpch.partsupp AS partsupp
  ON _s4.ps_suppkey = partsupp.ps_suppkey
JOIN tpch.part AS part
  ON part.p_container = 'LG DRUM'
  AND part.p_partkey = partsupp.ps_partkey
  AND part.p_retailprice < (
    CAST(_s4.sum_p_retailprice AS REAL) / _s4.sum_expr
  )
  AND part.p_retailprice < (
    partsupp.ps_supplycost * 1.5
  )
