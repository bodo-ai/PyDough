WITH _s1 AS (
  SELECT
    MIN(o_totalprice) AS min_o_totalprice,
    o_custkey
  FROM tpch.orders
  GROUP BY
    o_custkey
)
SELECT
  CASE WHEN customer.c_acctbal > 1000 THEN 'High' ELSE 'Low' END AS iff_col,
  customer.c_name IN ('Alice', 'Bob', 'Charlie') AS isin_col,
  COALESCE(_s1.min_o_totalprice, 0.0) AS default_val,
  NOT _s1.min_o_totalprice IS NULL AS has_acct_bal,
  _s1.min_o_totalprice IS NULL AS no_acct_bal,
  CASE WHEN customer.c_acctbal > 0 THEN customer.c_acctbal ELSE NULL END AS no_debt_bal
FROM tpch.customer AS customer
LEFT JOIN _s1 AS _s1
  ON _s1.o_custkey = customer.c_custkey
WHERE
  customer.c_acctbal <= 1000 AND customer.c_acctbal >= 100
