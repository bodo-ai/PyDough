SELECT
  nation.n_name AS nation_name,
  EXTRACT(YEAR FROM CAST(orders.o_orderdate AS TIMESTAMP)) AS order_year,
  EXTRACT(MONTH FROM CAST(orders.o_orderdate AS TIMESTAMP)) AS order_month,
  COUNT(*) AS n_orders
FROM tpch_s3_pq.tpch_sf1.region AS region
JOIN tpch_s3_pq.tpch_sf1.nation AS nation
  ON nation.n_regionkey = region.r_regionkey
JOIN tpch_s3_pq.tpch_sf1.customer AS customer
  ON customer.c_nationkey = nation.n_nationkey
JOIN tpch_s3_pq.tpch_sf1.orders AS orders
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
