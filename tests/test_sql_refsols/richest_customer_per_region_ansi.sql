WITH _t0 AS (
  SELECT
    customer.c_name AS name_6,
    customer.c_acctbal AS account_balance,
    nation.n_name AS nation_name,
    region.r_name AS region_name
  FROM tpch.region AS region
  JOIN tpch.nation AS nation
    ON nation.n_regionkey = region.r_regionkey
  JOIN tpch.customer AS customer
    ON customer.c_nationkey = nation.n_nationkey
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY region.r_regionkey ORDER BY customer.c_acctbal DESC NULLS FIRST, customer.c_name NULLS LAST) = 1
)
SELECT
  region_name,
  nation_name,
  name_6 AS customer_name,
  account_balance AS balance
FROM _t0
