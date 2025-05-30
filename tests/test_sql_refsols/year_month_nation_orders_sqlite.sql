WITH _t0 AS (
  SELECT
    COUNT() AS n_orders,
    nation.n_name AS nation_name,
    CAST(STRFTIME('%m', orders.o_orderdate) AS INTEGER) AS order_month,
    CAST(STRFTIME('%Y', orders.o_orderdate) AS INTEGER) AS order_year
  FROM tpch.region AS region
  JOIN tpch.nation AS nation
    ON nation.n_regionkey = region.r_regionkey
  JOIN tpch.customer AS customer
    ON customer.c_nationkey = nation.n_nationkey
  JOIN tpch.orders AS orders
    ON customer.c_custkey = orders.o_custkey AND orders.o_orderpriority = '1-URGENT'
  WHERE
    region.r_name IN ('ASIA', 'AFRICA')
  GROUP BY
    nation.n_name,
    CAST(STRFTIME('%m', orders.o_orderdate) AS INTEGER),
    CAST(STRFTIME('%Y', orders.o_orderdate) AS INTEGER)
)
SELECT
  nation_name,
  order_year,
  order_month,
  n_orders
FROM _t0
ORDER BY
  n_orders DESC
LIMIT 5
