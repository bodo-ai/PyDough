SELECT
  CONCAT_WS(' - ', customer.c_mktsegment, nation.n_name) AS label,
  COUNT(*) AS n
FROM snowflake_sample_data.tpch_sf1.customer AS customer
JOIN snowflake_sample_data.tpch_sf1.nation AS nation
  ON customer.c_nationkey = nation.n_nationkey
GROUP BY
  1
ORDER BY
  1 NULLS FIRST
