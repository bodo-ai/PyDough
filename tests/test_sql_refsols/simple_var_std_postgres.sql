WITH _s1 AS (
  SELECT
    s_nationkey,
    STDDEV_POP(s_acctbal) AS population_std_s_acctbal,
    VAR_POP(s_acctbal) AS population_var_s_acctbal,
    STDDEV(s_acctbal) AS sample_std_s_acctbal,
    VAR_SAMP(s_acctbal) AS sample_var_s_acctbal
  FROM tpch.supplier
  GROUP BY
    1
)
SELECT
  nation.n_name AS name,
  _s1.population_var_s_acctbal AS var,
  _s1.population_std_s_acctbal AS std,
  _s1.sample_var_s_acctbal AS sample_var,
  _s1.sample_std_s_acctbal AS sample_std,
  _s1.population_var_s_acctbal AS pop_var,
  _s1.population_std_s_acctbal AS pop_std
FROM tpch.nation AS nation
JOIN _s1 AS _s1
  ON _s1.s_nationkey = nation.n_nationkey
WHERE
  nation.n_name IN ('ALGERIA', 'ARGENTINA')
