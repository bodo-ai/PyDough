WITH _s2 AS (
  SELECT
    c_nationkey,
    COUNT(*) AS n_rows
  FROM snowflake_sample_data.tpch_sf1.customer
  GROUP BY
    1
)
SELECT
  CONCAT_WS(' - ', nation.n_name, region.r_name) AS label,
  SUM(_s2.n_rows) AS n_customers
FROM _s2 AS _s2
JOIN snowflake_sample_data.tpch_sf1.nation AS nation
  ON _s2.c_nationkey = nation.n_nationkey
JOIN snowflake_sample_data.tpch_sf1.region AS region
  ON nation.n_regionkey = region.r_regionkey
GROUP BY
  1
ORDER BY
  1 NULLS FIRST
