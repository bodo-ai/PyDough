WITH _t AS (
  SELECT
    customer.c_custkey AS key_2,
    nation.n_regionkey AS region_key,
    ROW_NUMBER() OVER (PARTITION BY nation.n_nationkey ORDER BY customer.c_acctbal DESC, customer.c_name) AS _w
  FROM tpch.nation AS nation
  JOIN tpch.customer AS customer
    ON customer.c_nationkey = nation.n_nationkey
), _s5 AS (
  SELECT
    COUNT() AS agg_0,
    _t.region_key
  FROM _t AS _t
  JOIN tpch.orders AS orders
    ON _t.key_2 = orders.o_custkey
  WHERE
    _t._w = 1
  GROUP BY
    _t.region_key
)
SELECT
  region.r_name AS region_name,
  COALESCE(_s5.agg_0, 0) AS n_orders
FROM tpch.region AS region
LEFT JOIN _s5 AS _s5
  ON _s5.region_key = region.r_regionkey
ORDER BY
  region_name
