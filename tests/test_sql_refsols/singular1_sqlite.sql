WITH _s1 AS (
  SELECT
    n_name AS name,
    n_regionkey AS region_key
  FROM tpch.nation
  WHERE
    n_nationkey = 4
)
SELECT
  region.r_name AS name,
  _s1.name AS nation_4_name
FROM tpch.region AS region
LEFT JOIN _s1 AS _s1
  ON _s1.region_key = region.r_regionkey
