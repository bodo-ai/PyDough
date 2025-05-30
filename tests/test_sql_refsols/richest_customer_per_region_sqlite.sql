WITH _t AS (
  SELECT
    customer.c_name AS name_6,
    customer.c_acctbal AS account_balance,
    nation.n_name AS nation_name,
    region.r_name AS region_name,
    ROW_NUMBER() OVER (PARTITION BY region.r_regionkey ORDER BY customer.c_acctbal DESC, customer.c_name) AS _w
  FROM tpch.region AS region
  JOIN tpch.nation AS nation
    ON nation.n_regionkey = region.r_regionkey
  JOIN tpch.customer AS customer
    ON customer.c_nationkey = nation.n_nationkey
)
SELECT
  region_name,
  nation_name,
  name_6 AS customer_name,
  account_balance AS balance
FROM _t
WHERE
  _w = 1
