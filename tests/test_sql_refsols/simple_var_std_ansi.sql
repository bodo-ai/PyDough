WITH _s1 AS (
  SELECT
    STDDEV_POP(s_acctbal) AS pop_std,
    VARIANCE_POP(s_acctbal) AS pop_var,
    STDDEV(s_acctbal) AS sample_std,
    VARIANCE(s_acctbal) AS sample_var,
    STDDEV_POP(s_acctbal) AS std,
    VARIANCE_POP(s_acctbal) AS var,
    s_nationkey AS nation_key
  FROM tpch.supplier
  GROUP BY
    s_nationkey
)
SELECT
  nation.n_name AS name,
  _s1.var,
  _s1.std,
  _s1.sample_var,
  _s1.sample_std,
  _s1.pop_var,
  _s1.pop_std
FROM tpch.nation AS nation
JOIN _s1 AS _s1
  ON _s1.nation_key = nation.n_nationkey
WHERE
  nation.n_name IN ('ALGERIA', 'ARGENTINA')
