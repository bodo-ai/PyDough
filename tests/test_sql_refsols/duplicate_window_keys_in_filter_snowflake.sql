WITH _t1 AS (
  SELECT
    o_orderkey,
    o_totalprice
  FROM snowflake_sample_data.tpch_sf1.orders
  QUALIFY
    ROW_NUMBER() OVER (ORDER BY o_totalprice DESC, o_orderkey) <= 3
)
SELECT
  o_orderkey AS key,
  o_totalprice AS total_price
FROM _t1
ORDER BY
  2 DESC NULLS LAST
