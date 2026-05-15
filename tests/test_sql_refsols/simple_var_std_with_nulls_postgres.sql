SELECT
  VAR_SAMP(CASE WHEN c_custkey > 3 THEN c_acctbal ELSE NULL END) AS var_samp_0_nnull,
  VAR_SAMP(CASE WHEN c_custkey > 2 THEN c_acctbal ELSE NULL END) AS var_samp_1_nnull,
  VAR_SAMP(CASE WHEN c_custkey > 1 THEN c_acctbal ELSE NULL END) AS var_samp_2_nnull,
  VAR_POP(CASE WHEN c_custkey > 3 THEN c_acctbal ELSE NULL END) AS var_pop_0_nnull,
  VAR_POP(CASE WHEN c_custkey > 2 THEN c_acctbal ELSE NULL END) AS var_pop_1_nnull,
  VAR_POP(CASE WHEN c_custkey > 1 THEN c_acctbal ELSE NULL END) AS var_pop_2_nnull,
  STDDEV(CASE WHEN c_custkey > 3 THEN c_acctbal ELSE NULL END) AS std_samp_0_nnull,
  STDDEV(CASE WHEN c_custkey > 2 THEN c_acctbal ELSE NULL END) AS std_samp_1_nnull,
  STDDEV(CASE WHEN c_custkey > 1 THEN c_acctbal ELSE NULL END) AS std_samp_2_nnull,
  STDDEV_POP(CASE WHEN c_custkey > 3 THEN c_acctbal ELSE NULL END) AS std_pop_0_nnull,
  STDDEV_POP(CASE WHEN c_custkey > 2 THEN c_acctbal ELSE NULL END) AS std_pop_1_nnull,
  STDDEV_POP(CASE WHEN c_custkey > 1 THEN c_acctbal ELSE NULL END) AS std_pop_2_nnull
FROM tpch.customer
WHERE
  c_custkey IN (1, 2, 3)
