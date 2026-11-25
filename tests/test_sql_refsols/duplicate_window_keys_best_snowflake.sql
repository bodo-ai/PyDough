WITH _t2 AS (
  SELECT
    o_custkey,
    o_orderkey
  FROM snowflake_sample_data.tpch_sf1.orders
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY o_custkey ORDER BY o_totalprice DESC, o_orderkey) = 1
)
SELECT
  customer.c_custkey AS key,
  _t2.o_orderkey AS best_order
FROM snowflake_sample_data.tpch_sf1.customer AS customer
LEFT JOIN _t2 AS _t2
  ON _t2.o_custkey = customer.c_custkey
WHERE
  customer.c_custkey <= 3
ORDER BY
  1 NULLS FIRST
