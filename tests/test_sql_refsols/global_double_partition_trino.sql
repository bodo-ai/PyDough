WITH _s0 AS (
  SELECT
    AVG(c_acctbal) AS avg_c_acctbal
  FROM tpch.customer
), _s2 AS (
  SELECT
    customer.c_mktsegment,
    customer.c_nationkey,
    MAX(
      CAST((
        100.0 * (
          customer.c_acctbal - _s0.avg_c_acctbal
        )
      ) AS DOUBLE) / _s0.avg_c_acctbal
    ) AS max_pct_diff
  FROM _s0 AS _s0
  CROSS JOIN tpch.customer AS customer
  GROUP BY
    1,
    2
), _t1 AS (
  SELECT
    _s2.c_mktsegment,
    MAX(_s2.max_pct_diff) AS max_max_pct_diff
  FROM _s2 AS _s2
  JOIN tpch.nation AS nation
    ON _s2.c_nationkey = nation.n_nationkey
  GROUP BY
    nation.n_name,
    1
)
SELECT
  c_mktsegment AS market_segment,
  ROUND(AVG(max_max_pct_diff), 2) AS avg_max_pct_diff
FROM _t1
GROUP BY
  1
