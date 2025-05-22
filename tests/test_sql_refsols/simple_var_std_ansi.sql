WITH _s1 AS (
  SELECT
    STDDEV_POP(s_acctbal) AS agg_0,
    VARIANCE_POP(s_acctbal) AS agg_1,
    STDDEV(s_acctbal) AS agg_2,
    VARIANCE(s_acctbal) AS agg_3,
    s_nationkey AS nation_key
  FROM tpch.supplier
  GROUP BY
    s_nationkey
)
SELECT
  nation.n_name AS name,
  _s1.agg_1 AS var,
  _s1.agg_0 AS std,
  _s1.agg_3 AS sample_var,
  _s1.agg_2 AS sample_std,
  _s1.agg_1 AS pop_var,
  _s1.agg_0 AS pop_std
FROM tpch.nation AS nation
LEFT JOIN _s1 AS _s1
  ON _s1.nation_key = nation.n_nationkey
WHERE
  nation.n_name IN ('ALGERIA', 'ARGENTINA')
