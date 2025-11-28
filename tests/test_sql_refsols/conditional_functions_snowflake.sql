SELECT
  IFF(ANY_VALUE(customer.c_acctbal) > 1000, 'High', 'Low') AS iff_col,
  ANY_VALUE(customer.c_name) IN ('Alice', 'Bob', 'Charlie') AS isin_col,
  COALESCE(MIN(orders.o_totalprice), 0.0) AS default_val,
  NOT MIN(orders.o_totalprice) IS NULL AS has_acct_bal,
  MIN(orders.o_totalprice) IS NULL AS no_acct_bal,
  CASE
    WHEN ANY_VALUE(customer.c_acctbal) > 0
    THEN ANY_VALUE(customer.c_acctbal)
    ELSE NULL
  END AS no_debt_bal
FROM tpch.customer AS customer
LEFT JOIN tpch.orders AS orders
  ON customer.c_custkey = orders.o_custkey
WHERE
  customer.c_acctbal <= 1000 AND customer.c_acctbal >= 100
GROUP BY
  customer.c_custkey
