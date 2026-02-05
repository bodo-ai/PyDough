WITH _S1 AS (
  SELECT
    o_custkey AS O_CUSTKEY,
    COUNT(*) AS N_ROWS
  FROM TPCH.ORDERS
  GROUP BY
    o_custkey
)
SELECT
  CUSTOMER.c_acctbal < 0 AS in_debt,
  _S1.N_ROWS <= 12 OR _S1.N_ROWS IS NULL AS at_most_12_orders,
  REGION.r_name = 'EUROPE' AS is_european,
  NATION.n_name <> 'GERMANY' AS non_german,
  CUSTOMER.c_acctbal > 0 AS non_empty_acct,
  NOT _S1.N_ROWS IS NULL AND _S1.N_ROWS >= 5 AS at_least_5_orders,
  REGION.r_name = 'ASIA' OR REGION.r_name = 'EUROPE' AS is_eurasian,
  CUSTOMER.c_acctbal < 0 AND REGION.r_name = 'EUROPE' AS is_european_in_debt
FROM TPCH.CUSTOMER CUSTOMER
LEFT JOIN _S1 _S1
  ON CUSTOMER.c_custkey = _S1.O_CUSTKEY
JOIN TPCH.NATION NATION
  ON CUSTOMER.c_nationkey = NATION.n_nationkey
JOIN TPCH.REGION REGION
  ON NATION.n_regionkey = REGION.r_regionkey
