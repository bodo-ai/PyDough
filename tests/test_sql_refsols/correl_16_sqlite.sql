WITH _s0 AS (
  SELECT
    NTILE(10000) OVER (ORDER BY s_acctbal, s_suppkey) AS tile,
    s_nationkey,
    s_suppkey
  FROM tpch.supplier
), _t AS (
  SELECT
    nation.n_nationkey,
    _s0.s_nationkey,
    _s0.s_suppkey,
    _s0.tile,
    NTILE(10000) OVER (PARTITION BY customer.c_nationkey, _s0.s_suppkey ORDER BY customer.c_acctbal, customer.c_custkey) AS _w
  FROM _s0 AS _s0
  JOIN tpch.nation AS nation
    ON _s0.s_nationkey = nation.n_nationkey
  JOIN tpch.region AS region
    ON nation.n_regionkey = region.r_regionkey AND region.r_name = 'EUROPE'
  JOIN tpch.customer AS customer
    ON customer.c_mktsegment = 'BUILDING' AND customer.c_nationkey = nation.n_nationkey
)
SELECT
  COUNT(DISTINCT s_suppkey) AS n
FROM _t
WHERE
  _w = tile AND n_nationkey = s_nationkey
