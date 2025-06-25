SELECT
  CONCAT_WS('-', region.r_name, nation.n_name) AS nation_name,
  PERCENTILE_DISC(1) WITHIN GROUP (ORDER BY
    orders.o_totalprice NULLS LAST) AS order_max,
  PERCENTILE_DISC(0) WITHIN GROUP (ORDER BY
    orders.o_totalprice NULLS LAST) AS order_min,
  PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY
    orders.o_totalprice NULLS LAST) AS order_median
FROM tpch.customer AS customer
JOIN tpch.nation AS nation
  ON customer.c_nationkey = nation.n_nationkey
JOIN tpch.region AS region
  ON nation.n_regionkey = region.r_regionkey
JOIN tpch.orders AS orders
  ON customer.c_custkey = orders.o_custkey
GROUP BY
  CONCAT_WS('-', region.r_name, nation.n_name)
