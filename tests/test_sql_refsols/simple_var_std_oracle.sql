SELECT
  ANY_VALUE(NATION.n_name) AS name,
  VAR_POP(SUPPLIER.s_acctbal) AS var,
  STDDEV_POP(SUPPLIER.s_acctbal) AS std,
  CASE
    WHEN COUNT(SUPPLIER.s_acctbal) < 2
    THEN NULL
    ELSE VARIANCE(SUPPLIER.s_acctbal)
  END AS sample_var,
  CASE
    WHEN COUNT(SUPPLIER.s_acctbal) < 2
    THEN NULL
    ELSE STDDEV(SUPPLIER.s_acctbal)
  END AS sample_std,
  VAR_POP(SUPPLIER.s_acctbal) AS pop_var,
  STDDEV_POP(SUPPLIER.s_acctbal) AS pop_std
FROM TPCH.NATION NATION
JOIN TPCH.SUPPLIER SUPPLIER
  ON NATION.n_nationkey = SUPPLIER.s_nationkey
WHERE
  NATION.n_name IN ('ALGERIA', 'ARGENTINA')
GROUP BY
  SUPPLIER.s_nationkey
