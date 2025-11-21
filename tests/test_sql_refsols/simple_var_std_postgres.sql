SELECT
  MAX(nation.n_name) AS name,
  VAR_POP(supplier.s_acctbal) AS var,
  STDDEV_POP(supplier.s_acctbal) AS std,
  VAR_SAMP(supplier.s_acctbal) AS sample_var,
  STDDEV(supplier.s_acctbal) AS sample_std,
  VAR_POP(supplier.s_acctbal) AS pop_var,
  STDDEV_POP(supplier.s_acctbal) AS pop_std
FROM tpch.nation AS nation
JOIN tpch.supplier AS supplier
  ON nation.n_nationkey = supplier.s_nationkey
WHERE
  nation.n_name IN ('ALGERIA', 'ARGENTINA')
GROUP BY
  supplier.s_nationkey
