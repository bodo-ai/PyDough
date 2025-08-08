WITH _s0 AS (
  SELECT
    NTILE(10000) OVER (ORDER BY s_acctbal NULLS LAST, s_suppkey NULLS LAST) AS tile,
    s_nationkey,
    s_suppkey
  FROM tpch.supplier
), _t1 AS (
  SELECT
    _s0.s_suppkey
  FROM _s0 AS _s0
  JOIN tpch.nation AS nation
    ON _s0.s_nationkey = nation.n_nationkey
  JOIN tpch.region AS region
    ON nation.n_regionkey = region.r_regionkey AND region.r_name = 'EUROPE'
  JOIN tpch.customer AS customer
    ON customer.c_mktsegment = 'BUILDING' AND customer.c_nationkey = nation.n_nationkey
  QUALIFY
    _s0.tile = NTILE(10000) OVER (PARTITION BY customer.c_nationkey, _s0.s_suppkey ORDER BY customer.c_acctbal NULLS LAST, customer.c_custkey NULLS LAST)
)
SELECT
  COUNT(DISTINCT s_suppkey) AS n
FROM _t1
