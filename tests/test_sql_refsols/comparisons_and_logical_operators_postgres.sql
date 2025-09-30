WITH _s1 AS (
  SELECT
    o_custkey,
    COUNT(*) AS n_rows
  FROM tpch.orders
  GROUP BY
    1
)
SELECT
  customer.c_acctbal < 0 AS in_debt,
  _s1.n_rows <= 12 OR _s1.n_rows IS NULL AS at_most_12_orders,
  region.r_name = 'EUROPE' AS is_european,
  nation.n_name <> 'GERMANY' AS non_german,
  customer.c_acctbal > 0 AS non_empty_acct,
  NOT _s1.n_rows IS NULL AND _s1.n_rows >= 5 AS at_least_5_orders,
  region.r_name = 'ASIA' OR region.r_name = 'EUROPE' AS is_eurasian,
  customer.c_acctbal < 0 AND region.r_name = 'EUROPE' AS is_european_in_debt
FROM tpch.customer AS customer
LEFT JOIN _s1 AS _s1
  ON _s1.o_custkey = customer.c_custkey
JOIN tpch.nation AS nation
  ON customer.c_nationkey = nation.n_nationkey
JOIN tpch.region AS region
  ON nation.n_regionkey = region.r_regionkey
