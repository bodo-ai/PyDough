SELECT
  CONCAT_WS('-', region.r_name, nation.n_name) AS nation_name,
  MAX(
    CASE
      WHEN ROW_NUMBER() OVER (PARTITION BY CONCAT_WS('-', region.r_name, nation.n_name) ORDER BY orders.o_totalprice DESC) > CAST((
        1.0 - 1
      ) * COUNT(orders.o_totalprice) OVER (PARTITION BY CONCAT_WS('-', region.r_name, nation.n_name)) AS INTEGER)
      THEN orders.o_totalprice
      ELSE NULL
    END
  ) AS order_max,
  MAX(
    CASE
      WHEN ROW_NUMBER() OVER (PARTITION BY CONCAT_WS('-', region.r_name, nation.n_name) ORDER BY orders.o_totalprice DESC) > CAST((
        1.0 - 0
      ) * COUNT(orders.o_totalprice) OVER (PARTITION BY CONCAT_WS('-', region.r_name, nation.n_name)) AS INTEGER)
      THEN orders.o_totalprice
      ELSE NULL
    END
  ) AS order_min,
  MAX(
    CASE
      WHEN ROW_NUMBER() OVER (PARTITION BY CONCAT_WS('-', region.r_name, nation.n_name) ORDER BY orders.o_totalprice DESC) > CAST((
        1.0 - 0.5
      ) * COUNT(orders.o_totalprice) OVER (PARTITION BY CONCAT_WS('-', region.r_name, nation.n_name)) AS INTEGER)
      THEN orders.o_totalprice
      ELSE NULL
    END
  ) AS order_median
FROM tpch.customer AS customer
JOIN tpch.nation AS nation
  ON customer.c_nationkey = nation.n_nationkey
JOIN tpch.region AS region
  ON nation.n_regionkey = region.r_regionkey
JOIN tpch.orders AS orders
  ON customer.c_custkey = orders.o_custkey
GROUP BY
  CONCAT_WS('-', region.r_name, nation.n_name)
