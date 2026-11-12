SELECT
  o_custkey AS key,
  COALESCE(SUM(o_totalprice), 0) AS total
FROM snowflake_sample_data.tpch_sf1.orders
WHERE
  o_custkey <= 3
GROUP BY
  1
