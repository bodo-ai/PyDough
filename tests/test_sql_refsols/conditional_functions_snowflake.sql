WITH _S1 AS (
  SELECT
    MIN(o_totalprice) AS MIN_O_TOTALPRICE,
    o_custkey AS O_CUSTKEY
  FROM TPCH.ORDERS
  GROUP BY
    o_custkey
)
SELECT
  IFF(CUSTOMER.c_acctbal > 1000, 'High', 'Low') AS iff_col,
  CUSTOMER.c_name IN ('Alice', 'Bob', 'Charlie') AS isin_col,
  COALESCE(_S1.MIN_O_TOTALPRICE, 0.0) AS default_val,
  NOT _S1.MIN_O_TOTALPRICE IS NULL AS has_acct_bal,
  _S1.MIN_O_TOTALPRICE IS NULL AS no_acct_bal,
  CASE WHEN CUSTOMER.c_acctbal > 0 THEN CUSTOMER.c_acctbal ELSE NULL END AS no_debt_bal
FROM TPCH.CUSTOMER AS CUSTOMER
LEFT JOIN _S1 AS _S1
  ON CUSTOMER.c_custkey = _S1.O_CUSTKEY
WHERE
  CUSTOMER.c_acctbal <= 1000 AND CUSTOMER.c_acctbal >= 100
