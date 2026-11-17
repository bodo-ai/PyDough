SELECT
  ANY_VALUE(nation.n_name) AS name,
  VARIANCE_POP(supplier.s_acctbal) AS var,
  STDDEV_POP(supplier.s_acctbal) AS std,
  VARIANCE(supplier.s_acctbal) AS sample_var,
  STDDEV(supplier.s_acctbal) AS sample_std,
  VARIANCE_POP(supplier.s_acctbal) AS pop_var,
  STDDEV_POP(supplier.s_acctbal) AS pop_std
FROM tpch.nation AS nation
JOIN tpch.supplier AS supplier
  ON nation.n_nationkey = supplier.s_nationkey
WHERE
  nation.n_name IN ('ALGERIA', 'ARGENTINA')
GROUP BY
  supplier.s_nationkey
