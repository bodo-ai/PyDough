WITH _s1 AS (
  SELECT
    STDDEV_POP(s_acctbal) AS pop_std,
    VARIANCE_POP(s_acctbal) AS pop_var,
    STDDEV(s_acctbal) AS sample_std,
    VARIANCE(s_acctbal) AS sample_var,
    s_nationkey
  FROM tpch.supplier
  GROUP BY
    5
)
SELECT
  nation.n_name AS name,
  _s1.pop_var AS var,
  _s1.pop_std AS std,
  _s1.sample_var,
  _s1.sample_std,
  _s1.pop_var,
  _s1.pop_std
FROM tpch.nation AS nation
JOIN _s1 AS _s1
  ON _s1.s_nationkey = nation.n_nationkey
WHERE
  nation.n_name IN ('ALGERIA', 'ARGENTINA')
