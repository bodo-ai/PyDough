WITH _s1 AS (
  SELECT
    MIN(o_totalprice) AS min_o_totalprice,
    o_custkey
  FROM tpch.ORDERS
  GROUP BY
    2
)
SELECT
  CASE WHEN CUSTOMER.c_acctbal > 1000 THEN 'High' ELSE 'Low' END AS iff_col,
  CUSTOMER.c_name IN ('Alice', 'Bob', 'Charlie') AS isin_col,
  COALESCE(_s1.min_o_totalprice, 0.0) AS default_val,
  NOT _s1.min_o_totalprice IS NULL AS has_acct_bal,
  _s1.min_o_totalprice IS NULL AS no_acct_bal,
  CASE WHEN CUSTOMER.c_acctbal > 0 THEN CUSTOMER.c_acctbal ELSE NULL END AS no_debt_bal
FROM tpch.CUSTOMER AS CUSTOMER
LEFT JOIN _s1 AS _s1
  ON CUSTOMER.c_custkey = _s1.o_custkey
WHERE
  CUSTOMER.c_acctbal <= 1000 AND CUSTOMER.c_acctbal >= 100
