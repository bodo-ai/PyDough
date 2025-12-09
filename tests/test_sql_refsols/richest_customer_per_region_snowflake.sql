WITH _t0 AS (
  SELECT
    customer.c_acctbal,
    customer.c_name,
    nation.n_name,
    region.r_name
  FROM tpch.region AS region
  JOIN tpch.nation AS nation
    ON nation.n_regionkey = region.r_regionkey
  JOIN tpch.customer AS customer
    ON customer.c_nationkey = nation.n_nationkey
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY n_regionkey ORDER BY customer.c_acctbal DESC, customer.c_name) = 1
)
SELECT
  r_name AS region_name,
  n_name AS nation_name,
  c_name AS customer_name,
  c_acctbal AS balance
FROM _t0
