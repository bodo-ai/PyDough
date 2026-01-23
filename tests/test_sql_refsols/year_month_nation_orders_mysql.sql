SELECT
  NATION.n_name AS nation_name,
  EXTRACT(YEAR FROM CAST(ORDERS.o_orderdate AS DATETIME)) AS order_year,
  EXTRACT(MONTH FROM CAST(ORDERS.o_orderdate AS DATETIME)) AS order_month,
  COUNT(*) AS n_orders
FROM tpch.REGION AS REGION
JOIN tpch.NATION AS NATION
  ON NATION.n_regionkey = REGION.r_regionkey
JOIN tpch.CUSTOMER AS CUSTOMER
  ON CUSTOMER.c_nationkey = NATION.n_nationkey
JOIN tpch.ORDERS AS ORDERS
  ON CUSTOMER.c_custkey = ORDERS.o_custkey AND ORDERS.o_orderpriority = '1-URGENT'
WHERE
  REGION.r_name IN ('ASIA', 'AFRICA')
GROUP BY
  1,
  2,
  3
ORDER BY
  4 DESC
LIMIT 5
