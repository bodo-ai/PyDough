WITH _s1 AS (
  SELECT
    o_custkey,
    MIN(o_totalprice) AS min_ototalprice
  FROM tpch.orders
  GROUP BY
    1
)
SELECT
  IFF(customer.c_acctbal > 1000, 'High', 'Low') AS iff_col,
  customer.c_name IN ('Alice', 'Bob', 'Charlie') AS isin_col,
  COALESCE(_s1.min_ototalprice, 0.0) AS default_val,
  NOT _s1.min_ototalprice IS NULL AS has_acct_bal,
  _s1.min_ototalprice IS NULL AS no_acct_bal,
  CASE WHEN customer.c_acctbal > 0 THEN customer.c_acctbal ELSE NULL END AS no_debt_bal
FROM tpch.customer AS customer
LEFT JOIN _s1 AS _s1
  ON _s1.o_custkey = customer.c_custkey
WHERE
  customer.c_acctbal <= 1000 AND customer.c_acctbal >= 100
