WITH _s1 AS (
  SELECT
    COUNT(*) AS n_rows,
    o_custkey
  FROM tpch.ORDERS
  GROUP BY
    o_custkey
)
SELECT
  CUSTOMER.c_acctbal < 0 AS in_debt,
  _s1.n_rows <= 12 OR _s1.n_rows IS NULL AS at_most_12_orders,
  REGION.r_name = 'EUROPE' AS is_european,
  NATION.n_name <> 'GERMANY' AS non_german,
  CUSTOMER.c_acctbal > 0 AS non_empty_acct,
  NOT _s1.n_rows IS NULL AND _s1.n_rows >= 5 AS at_least_5_orders,
  REGION.r_name = 'ASIA' OR REGION.r_name = 'EUROPE' AS is_eurasian,
  CUSTOMER.c_acctbal < 0 AND REGION.r_name = 'EUROPE' AS is_european_in_debt
FROM tpch.CUSTOMER AS CUSTOMER
LEFT JOIN _s1 AS _s1
  ON CUSTOMER.c_custkey = _s1.o_custkey
JOIN tpch.NATION AS NATION
  ON CUSTOMER.c_nationkey = NATION.n_nationkey
JOIN tpch.REGION AS REGION
  ON NATION.n_regionkey = REGION.r_regionkey
