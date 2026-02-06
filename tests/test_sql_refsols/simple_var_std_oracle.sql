SELECT
  ANY_VALUE(NATION.n_name) AS name,
  VARIANCE_POP(SUPPLIER.s_acctbal) AS var,
  STDDEV_POP(SUPPLIER.s_acctbal) AS std,
  VARIANCE(SUPPLIER.s_acctbal) AS sample_var,
  STDDEV(SUPPLIER.s_acctbal) AS sample_std,
  VARIANCE_POP(SUPPLIER.s_acctbal) AS pop_var,
  STDDEV_POP(SUPPLIER.s_acctbal) AS pop_std
FROM TPCH.NATION NATION
JOIN TPCH.SUPPLIER SUPPLIER
  ON NATION.n_nationkey = SUPPLIER.s_nationkey
WHERE
  NATION.n_name IN ('ALGERIA', 'ARGENTINA')
GROUP BY
  SUPPLIER.s_nationkey
