WITH _t3 AS (
  SELECT
    customer.c_custkey AS key_2,
    nation.n_regionkey AS region_key
  FROM tpch.nation AS nation
  JOIN tpch.customer AS customer
    ON customer.c_nationkey = nation.n_nationkey
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY nation.n_nationkey ORDER BY customer.c_acctbal DESC NULLS FIRST, customer.c_name NULLS LAST) = 1
), _s5 AS (
  SELECT
    COUNT() AS agg_0,
    _t3.region_key
  FROM _t3 AS _t3
  JOIN tpch.orders AS orders
    ON _t3.key_2 = orders.o_custkey
  GROUP BY
    _t3.region_key
)
SELECT
  region.r_name AS region_name,
  COALESCE(_s5.agg_0, 0) AS n_orders
FROM tpch.region AS region
LEFT JOIN _s5 AS _s5
  ON _s5.region_key = region.r_regionkey
ORDER BY
  region_name
