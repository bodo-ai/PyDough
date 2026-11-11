WITH _s1 AS (
  SELECT
    n_name,
    n_regionkey
  FROM snowflake_sample_data.tpch_sf1.nation
  WHERE
    n_nationkey = 4
)
SELECT
  region.r_name AS name,
  _s1.n_name AS nation_4_name
FROM snowflake_sample_data.tpch_sf1.region AS region
LEFT JOIN _s1 AS _s1
  ON _s1.n_regionkey = region.r_regionkey
