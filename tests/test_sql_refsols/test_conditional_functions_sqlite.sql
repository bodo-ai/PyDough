SELECT
  IIF(c_acctbal > 1000, 'High', 'Low') AS iff_col,
  c_name IN ('Alice', 'Bob', 'Charlie') AS isin_col,
  COALESCE(c_acctbal, 0.0) AS default_val,
  NOT c_acctbal IS NULL AS has_acct_bal,
  c_acctbal IS NULL AS no_acct_bal,
  CASE WHEN c_acctbal > 0 THEN c_acctbal ELSE NULL END AS no_debt_bal
FROM tpch.customer
WHERE
  c_acctbal <= 1000 AND c_acctbal >= 100
