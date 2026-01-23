WITH _t AS (
  SELECT
    customer.c_acctbal,
    customer.c_name,
    nation.n_name,
    region.r_name,
    ROW_NUMBER() OVER (PARTITION BY nation.n_regionkey ORDER BY customer.c_acctbal DESC, customer.c_name) AS _w
  FROM tpch.region AS region
  JOIN tpch.nation AS nation
    ON nation.n_regionkey = region.r_regionkey
  JOIN tpch.customer AS customer
    ON customer.c_nationkey = nation.n_nationkey
)
SELECT
  r_name AS region_name,
  n_name AS nation_name,
  c_name AS customer_name,
  c_acctbal AS balance
FROM _t
WHERE
  _w = 1
