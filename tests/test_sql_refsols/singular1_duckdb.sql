WITH _s1 AS (
  SELECT
    n_name,
    n_regionkey
  FROM tpch.nation
  WHERE
    n_nationkey = 4
)
SELECT
  region.r_name AS name,
  _s1.n_name AS nation_4_name
FROM tpch.region AS region
LEFT JOIN _s1 AS _s1
  ON _s1.n_regionkey = region.r_regionkey
