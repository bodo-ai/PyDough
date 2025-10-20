WITH _s1 AS (
  SELECT
    o_custkey,
    o_totalprice
  FROM tpch.orders
)
SELECT
  CASE WHEN MAX(customer.c_acctbal) > 1000 THEN 'High' ELSE 'Low' END AS iff_col,
  MAX(customer.c_name) IN ('Alice', 'Bob', 'Charlie') AS isin_col,
  COALESCE(MIN(_s1.o_totalprice), 0.0) AS default_val,
  NOT MIN(_s1.o_totalprice) IS NULL AS has_acct_bal,
  MIN(_s1.o_totalprice) IS NULL AS no_acct_bal,
  CASE WHEN MAX(customer.c_acctbal) > 0 THEN MAX(customer.c_acctbal) ELSE NULL END AS no_debt_bal
FROM tpch.customer AS customer
LEFT JOIN _s1 AS _s1
  ON _s1.o_custkey = customer.c_custkey
WHERE
  customer.c_acctbal <= 1000 AND customer.c_acctbal >= 100
GROUP BY
  _s1.o_custkey
