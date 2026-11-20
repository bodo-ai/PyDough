WITH _t1 AS (
  SELECT
    1 AS `_`
  FROM tpch_s3_pq.tpch_sf1.orders
  QUALIFY
    RANK() OVER (ORDER BY o_orderdate NULLS LAST) = 1
)
SELECT
  COUNT(*) AS n_orders
FROM _t1
