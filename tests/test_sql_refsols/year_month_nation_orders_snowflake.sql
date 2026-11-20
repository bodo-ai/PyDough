SELECT
  nation.n_name AS nation_name,
  YEAR(CAST(orders.o_orderdate AS TIMESTAMP)) AS order_year,
  MONTH(CAST(orders.o_orderdate AS TIMESTAMP)) AS order_month,
  COUNT(*) AS n_orders
FROM snowflake_sample_data.tpch_sf1.region AS region
JOIN snowflake_sample_data.tpch_sf1.nation AS nation
  ON nation.n_regionkey = region.r_regionkey
JOIN snowflake_sample_data.tpch_sf1.customer AS customer
  ON customer.c_nationkey = nation.n_nationkey
JOIN snowflake_sample_data.tpch_sf1.orders AS orders
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
