SELECT
  nation.n_name AS nation_name,
  CAST(STRFTIME('%Y', orders.o_orderdate) AS INTEGER) AS order_year,
  CAST(STRFTIME('%m', orders.o_orderdate) AS INTEGER) AS order_month,
  COUNT(*) AS n_orders
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
  1,
  2,
  3
ORDER BY
  4 DESC
LIMIT 5
