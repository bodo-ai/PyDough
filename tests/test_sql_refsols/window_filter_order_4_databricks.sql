WITH _t2 AS (
  SELECT
    COUNT(*) AS n_rows
  FROM tpch_s3_pq.tpch_sf1.customer AS customer
  JOIN tpch_s3_pq.tpch_sf1.nation AS nation
    ON customer.c_nationkey = nation.n_nationkey AND nation.n_name = 'GERMANY'
  JOIN tpch_s3_pq.tpch_sf1.orders AS orders
    ON EXTRACT(YEAR FROM CAST(orders.o_orderdate AS TIMESTAMP)) = 1992
    AND customer.c_custkey = orders.o_custkey
  GROUP BY
    orders.o_custkey
), _t1 AS (
  SELECT
    1 AS `_`
  FROM _t2
  QUALIFY
    n_rows < AVG(CAST(n_rows AS DOUBLE)) OVER ()
)
SELECT
  COUNT(*) AS n
FROM _t1
