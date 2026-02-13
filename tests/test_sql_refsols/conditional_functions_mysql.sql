SELECT
  CASE WHEN ANY_VALUE(CUSTOMER.c_acctbal) > 1000 THEN 'High' ELSE 'Low' END AS iff_col,
  ANY_VALUE(CUSTOMER.c_name) IN ('Alice', 'Bob', 'Charlie') AS isin_col,
  COALESCE(MIN(ORDERS.o_totalprice), 0.0) AS default_val,
  NOT MIN(ORDERS.o_totalprice) IS NULL AS has_acct_bal,
  MIN(ORDERS.o_totalprice) IS NULL AS no_acct_bal,
  CASE
    WHEN ANY_VALUE(CUSTOMER.c_acctbal) > 0
    THEN ANY_VALUE(CUSTOMER.c_acctbal)
    ELSE NULL
  END AS no_debt_bal
FROM tpch.CUSTOMER AS CUSTOMER
LEFT JOIN tpch.ORDERS AS ORDERS
  ON CUSTOMER.c_custkey = ORDERS.o_custkey
WHERE
  CUSTOMER.c_acctbal <= 1000 AND CUSTOMER.c_acctbal >= 100
GROUP BY
  CUSTOMER.c_custkey
