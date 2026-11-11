WITH _t1 AS (
  SELECT
    1 AS "_"
  FROM snowflake_sample_data.tpch_sf1.orders AS orders
  JOIN snowflake_sample_data.tpch_sf1.customer AS customer
    ON customer.c_custkey = orders.o_custkey AND customer.c_mktsegment = 'BUILDING'
  WHERE
    YEAR(CAST(orders.o_orderdate AS TIMESTAMP)) = 1995
  QUALIFY
    customer.c_acctbal < AVG(CAST(customer.c_acctbal AS DOUBLE)) OVER ()
)
SELECT
  COUNT(*) AS n
FROM _t1
