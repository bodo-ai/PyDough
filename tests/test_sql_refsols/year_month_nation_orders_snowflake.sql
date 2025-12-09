SELECT
  nation.n_name AS nation_name,
  YEAR(CAST(orders.o_orderdate AS TIMESTAMP)) AS order_year,
  MONTH(CAST(orders.o_orderdate AS TIMESTAMP)) AS order_month,
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
  4 DESC NULLS LAST
LIMIT 5
